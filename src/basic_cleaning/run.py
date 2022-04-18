#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()

def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact

    ######################
    # YOUR CODE HERE     #
    ######################
    logger.info(f"Loading data stored at {args.input_artifact}.")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    
    logger.info("Performing preprocessing . . .")
    df = pd.read_csv(artifact_local_path)
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()
    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info(f"Uploading artifact as {args.output_artifact}.")
    df.to_csv(args.output_artifact)
    artifact = wandb.Artifact(name = args.output_artifact, type = args.output_type, description = args.output_description)
    artifact.add_file(args.output_artifact)
    run.log_artifact(artifact)
    run.finish()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning")

    parser.add_argument(
        "--input_artifact", 
        type = str,
        help = "Path to the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type = str,
        help = "Name of the produced artifact",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type = str,
        help = "Type of generated artifact",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type = str, 
        help = "Description of the output artifact",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type = float,## INSERT TYPE HERE: str, float or int,
        help = "Minimum price allowed in the data",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type = float,
        help = "Maximum price allowed in the data",
        required=True
    )


    args = parser.parse_args()

    go(args)
