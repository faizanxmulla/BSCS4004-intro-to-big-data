from google.cloud import storage


def count_lines(event, context):
    """Triggered by a change to a Cloud Storage bucket."""
    file_name = event["name"]
    bucket_name = event["bucket"]

    # Ignore files that start with "results_"
    if file_name.startswith("results_"):
        print(f"Ignoring file {file_name} as it starts with 'results_'")
        return

    # Initialize client
    storage_client = storage.Client()

    # Get bucket and file
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(file_name)

    # Read content and count lines
    content = blob.download_as_text()
    line_count = len(content.splitlines())

    # Create result message
    result = f"File {file_name} contains {line_count} lines"
    print(result)  # This will appear in logs

    # Save result to new file
    result_blob = bucket.blob(f"results_{file_name}")
    result_blob.upload_from_string(result)

    return result
