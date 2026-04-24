import json
import psycopg2
from datetime import datetime
import logging
import torch
import boto3
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")
secrets_client = boto3.client("secretsmanager")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# FinBERT MODEL CACHING - Se carga UNA SOLA VEZ al iniciar Lambda
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

logger.info("Loading FinBERT model from HuggingFace...")

# FinBERT: BERT entrenado en ~4.3B tokens de texto financiero
# Optimizado específicamente para análisis de sentimiento en finanzas
MODEL_NAME = "ProsusAI/finbert"

try:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForSequenceClassification.from_pretrained(MODEL_NAME)
    model.eval()  # Modo evaluación (optimizado para inferencia)

    # Usar CPU si no hay GPU disponible
    DEVICE = torch.device("cpu")
    model.to(DEVICE)

    logger.info("✓ FinBERT model loaded successfully")
    logger.info(f"  - Model: {MODEL_NAME}")
    logger.info(f"  - Device: {DEVICE}")
    logger.info(
        f"  - Model size: {sum(p.numel() for p in model.parameters()) / 1e6:.1f}M parameters"
    )

except Exception as e:
    logger.error(f"Failed to load FinBERT model: {str(e)}")
    raise

ANTHROPIC_API_KEY = "PEGAR_AQUI_LA_API_KEY"


def get_secret(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        return json.loads(response["SecretBinary"])
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def read_news_from_s3():
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        response = s3_client.get_object(
            Bucket="tfm-unir-datalake", Key=f"raw/{today}/news.json"
        )
        news_data = json.loads(response["Body"].read())
        logger.info(f"✓ Read news from S3: {len(news_data)} tickers")
        return news_data
    except Exception as e:
        logger.error(f"Error reading news from S3: {str(e)}")
        raise


def analyze_sentiment_batch(headlines):
    """
    Analyze sentiment for multiple headlines using FinBERT in batch mode.
    Batch processing is much faster than processing headlines individually.

    FinBERT output classes:
    - 0: negative (map to 'bearish')
    - 1: neutral (map to 'neutral')
    - 2: positive (map to 'bullish')
    """
    if not headlines:
        return []

    try:
        logger.debug(f"Analyzing {len(headlines)} headlines in batch mode")

        # Tokenize all headlines at once (batch processing)
        inputs = tokenizer(
            headlines,
            return_tensors="pt",
            padding=True,
            truncation=True,
            max_length=512,
        )

        # Move to same device as model
        inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

        # Run inference with no_grad for faster computation
        with torch.no_grad():
            outputs = model(**inputs)

        # Extract logits and compute probabilities
        logits = outputs.logits
        probabilities = torch.softmax(logits, dim=-1)

        # Map FinBERT classes to trading sentiment
        sentiment_map = {
            0: "bearish",  # negative → bearish
            1: "neutral",  # neutral → neutral
            2: "bullish",  # positive → bullish
        }

        results = []

        for i, headline in enumerate(headlines):
            predicted_class = torch.argmax(probabilities[i]).item()
            confidence = float(probabilities[i][predicted_class])
            sentiment = sentiment_map[predicted_class]

            # Create justification based on the model's confidence
            confidence_pct = confidence * 100
            if confidence > 0.85:
                strength = "strong"
            elif confidence > 0.70:
                strength = "moderate"
            else:
                strength = "weak"

            justification = (
                f"FinBERT financial sentiment model confidence: {confidence_pct:.1f}% "
                f"({strength} {sentiment})"
            )

            results.append(
                {
                    "headline": headline,
                    "sentiment": sentiment,
                    "confidence": confidence,
                    "justification": justification,
                }
            )

        logger.debug(f"✓ Batch analysis completed for {len(results)} headlines")
        return results

    except Exception as e:
        logger.error(f"Error in batch sentiment analysis: {str(e)}")
        raise


def insert_sentiment_scores(connection, batch_date, ticker, sentiment_results):
    """Insert sentiment analysis results into Aurora PostgreSQL"""
    try:
        cursor = connection.cursor()

        for result in sentiment_results:
            query = """
                INSERT INTO sentiment_scores
                (batch_date, ticker, headline, sentiment, confidence, justification)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            cursor.execute(
                query,
                (
                    batch_date,
                    ticker,
                    result["headline"],
                    result["sentiment"],
                    result["confidence"],
                    result["justification"],
                ),
            )

        connection.commit()
        cursor.close()
        logger.info(
            f"✓ Inserted {len(sentiment_results)} sentiment scores for {ticker}"
        )

    except Exception as e:
        logger.error(f"Error inserting sentiment scores for {ticker}: {str(e)}")
        raise


def handler(event, context):
    """
    Main Lambda handler for sentiment analysis using FinBERT

    Performance characteristics:
    - Batch processing: ~15-30ms per headline
    - For 1000 headlines: ~15-30 seconds total
    - Cost: Only Lambda compute (no external API calls)
    """
    try:
        logger.info("=" * 70)
        logger.info("Lambda Sentiment Analysis Started (FinBERT)")
        logger.info("=" * 70)

        start_time = datetime.now()

        # Get Aurora credentials
        aurora_creds = get_secret("aurora/credentials")
        logger.info("✓ Retrieved Aurora credentials from Secrets Manager")

        # Read news from S3
        news_data = read_news_from_s3()

        if not news_data:
            logger.warning("No news data found")
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": "No news to analyze", "headlines_processed": 0}
                ),
            }

        today = datetime.now().strftime("%Y-%m-%d")

        connection = psycopg2.connect(
            host=aurora_creds["host"],
            port=aurora_creds["port"],
            user=aurora_creds["username"],
            password=aurora_creds["password"],
            database=aurora_creds["dbname"],
        )
        logger.info("✓ Connected to Aurora PostgreSQL")

        total_headlines = 0
        processed_headlines = 0
        skipped_headlines = 0
        tickers_processed = 0

        # Process each ticker's news
        for ticker, headlines_list in news_data.items():
            try:
                if not headlines_list:
                    logger.warning(f"No headlines for ticker {ticker}")
                    continue

                ticker_start = datetime.now()

                # Extract headline texts
                headlines_text = [
                    h.get("headline", "")
                    for h in headlines_list
                    if h.get("headline", "").strip()
                ]

                if not headlines_text:
                    logger.warning(f"No valid headlines for ticker {ticker}")
                    skipped_headlines += len(headlines_list)
                    continue

                total_headlines += len(headlines_text)

                # ⚡ BATCH ANALYSIS: Process all headlines for this ticker at once
                # This is much faster than individual processing
                sentiment_results = analyze_sentiment_batch(headlines_text)

                if sentiment_results is None or len(sentiment_results) == 0:
                    logger.warning(f"Failed to analyze headlines for {ticker}")
                    skipped_headlines += len(headlines_text)
                    continue

                # Insert into database
                insert_sentiment_scores(connection, today, ticker, sentiment_results)

                processed_headlines += len(sentiment_results)
                tickers_processed += 1

                ticker_time = (datetime.now() - ticker_start).total_seconds()
                avg_time_per_headline = (ticker_time / len(sentiment_results)) * 1000

                logger.info(
                    f"✓ {ticker}: {len(sentiment_results)} headlines analyzed "
                    f"in {ticker_time:.2f}s ({avg_time_per_headline:.1f}ms/headline)"
                )

                # Sample output for first ticker (for debugging)
                if tickers_processed == 1:
                    logger.info(f"  Sample results for {ticker}:")
                    for result in sentiment_results[:2]:
                        logger.info(
                            f"    - '{result['headline'][:60]}...' "
                            f"→ {result['sentiment'].upper()} ({result['confidence']:.1%})"
                        )

            except Exception as e:
                logger.error(f"Error processing {ticker}: {str(e)}")
                skipped_headlines += len(headlines_list) if headlines_list else 0
                continue

        connection.close()

        total_time = (datetime.now() - start_time).total_seconds()

        logger.info("=" * 70)
        logger.info("Sentiment Analysis Completed")
        logger.info("=" * 70)
        logger.info("Summary:")
        logger.info(f"  - Tickers processed: {tickers_processed}")
        logger.info(f"  - Headlines processed: {processed_headlines}")
        logger.info(f"  - Headlines skipped: {skipped_headlines}")
        logger.info(f"  - Total headlines: {total_headlines}")
        logger.info(f"  - Total time: {total_time:.2f}s")
        if processed_headlines > 0:
            logger.info(
                f"  - Average: {(total_time/processed_headlines)*1000:.1f}ms/headline"
            )
        logger.info("=" * 70)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Sentiment analysis completed",
                    "tickers_processed": tickers_processed,
                    "total_headlines": total_headlines,
                    "processed_headlines": processed_headlines,
                    "skipped_headlines": skipped_headlines,
                    "execution_time_seconds": total_time,
                    "cost": "$0.00 (FinBERT is free!)",
                }
            ),
        }
    except Exception as e:
        logger.error(f"Fatal error in handler: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "error_type": type(e).__name__}),
        }
