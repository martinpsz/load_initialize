# Steps to set up Python environment:

1. From this directory, run 'source venv/bin/activate' in a terminal
2. Run 'which pip'/'which python' in your terminal to confirm that each is linked to the the environment in venv.
3. Once you confirm that your 'pip' is linked to venv environment, run 'pip install -r requirements.txt' to install the dependencies.

# Set up environment variables:

1. In the root directory, create a .env file and in it, add your AWS Credentials, which should include values for the following:
    - AWS_REGION
    - AWS_ACCESS_KEY_ID
    - AWS_SECRET_ACCESS_KEY
    - AWS_SESSION_TOKEN

# Main.py

This file has two key elements: (1) creating a local list of the filepaths in s3 buckets to access archive, production links for various enterprise events (commented out section between lines 33-52) and (2) the main function that loops through each link from (1) and creates a deduplicated Python dictionary that is then used to create tsvs with records for all the various enterprise events.

