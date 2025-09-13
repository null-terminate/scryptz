# CloudWatch Logs Querier

A Python script to query AWS CloudWatch Logs using Logs Insights and save results to files.

## Features

- Query CloudWatch Logs using Logs Insights syntax
- Support for multiple log groups
- **Streaming output** - handles millions of rows efficiently by writing results as they arrive
- Automatic file splitting when results exceed 5000 lines
- Flexible datetime parsing
- **JSONL formatted output** - newline-delimited JSON for easy processing
- **Filters out @ptr fields** automatically
- Progress indicators for large datasets

## Configuration

Before using the script, update the hardcoded values in `main.py`:

```python
AWS_ACCOUNT_ID = "123456789012"  # Replace with your AWS account ID
AWS_REGION = "us-east-1"  # Replace with your preferred region
LOG_GROUPS = [
    "/aws/lambda/my-function",  # Replace with your log group(s)
    # "/aws/apigateway/my-api",  # Add more log groups as needed
]
```

## Prerequisites

1. AWS credentials configured (via AWS CLI, environment variables, or IAM role)
2. Appropriate permissions for CloudWatch Logs (logs:StartQuery, logs:GetQueryResults)

## Usage

```bash
python main.py --start-time "2024-01-01 00:00:00" --end-time "2024-01-01 23:59:59" --query "fields @timestamp, @message | sort @timestamp desc | limit 1000"
```

### Arguments

- `--start-time`: Start time for the query (required)
  - Formats: `YYYY-MM-DD HH:MM:SS`, `YYYY-MM-DD HH:MM`, `YYYY-MM-DD`
- `--end-time`: End time for the query (required)
  - Same formats as start-time
- `--query`: CloudWatch Logs Insights query string (required)
- `--output-prefix`: Prefix for output files (optional, default: "cloudwatch_logs")

### Example Queries

1. **Get all logs with timestamp and message:**
   ```
   "fields @timestamp, @message | sort @timestamp desc | limit 1000"
   ```

2. **Filter for ERROR logs:**
   ```
   "fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc"
   ```

3. **Get logs with specific log level:**
   ```
   "fields @timestamp, @message, @logLevel | filter @logLevel = \"ERROR\" | sort @timestamp desc"
   ```

4. **Count logs by 5-minute intervals:**
   ```
   "stats count() by bin(5m)"
   ```

5. **Filter by specific request ID:**
   ```
   "fields @timestamp, @message | filter @requestId = \"abc-123-def\" | sort @timestamp desc"
   ```

## Output

Results are saved to the `output/` directory with the following naming convention:
- `{output-prefix}_001.jsonl`
- `{output-prefix}_002.jsonl`
- etc.

Each file contains up to 5000 lines of newline-delimited JSON (JSONL) log entries. Each line is a single, compact JSON object representing one log entry. The `@ptr` fields are automatically filtered out.

**Example output format:**
```jsonl
{"@timestamp":"2024-01-15T10:30:45.123Z","@message":"Request processed successfully","requestId":"abc-123"}
{"@timestamp":"2024-01-15T10:30:46.456Z","@message":"Processing request","userId":"user456"}
```

## Example Usage

```bash
# Query last 24 hours of logs for errors
python main.py \
  --start-time "2024-01-15 00:00:00" \
  --end-time "2024-01-16 00:00:00" \
  --query "fields @timestamp, @message | filter @message like /ERROR/ | sort @timestamp desc" \
  --output-prefix "error_logs"

# Query specific time range for all logs
python main.py \
  --start-time "2024-01-15 14:30:00" \
  --end-time "2024-01-15 15:30:00" \
  --query "fields @timestamp, @message | sort @timestamp desc | limit 5000" \
  --output-prefix "hourly_logs"
```

## Error Handling

The script includes comprehensive error handling for:
- AWS API errors
- Query timeouts (default: 5 minutes)
- Invalid datetime formats
- File I/O errors

## Dependencies

All required dependencies are listed in `pyproject.toml`:
- boto3
- botocore
- python-dateutil

Install with:
```bash
pip install -e .
