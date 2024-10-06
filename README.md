# LLM Data Processing and Training Project

## Video Link

https://youtu.be/OMtApRDT_mU

## Description

This is part of my submission for Assignment 1 for CS441 (39827) Engineering Distributed Objects for Cloud Computing.
The project is to train an LLM Encoder using Neural Networks and Distributed Objects.\
\
The Project uses Hadoop MapReduce to achieve Parallel Processing. The project is run on AWS EMR (Elastic MapReduce). The Neural Network model used is a SkipGram Model configured in Tensorflow. Tokenization is obtained using JTokkit.


## Overview

This project is designed to process large text datasets for training or fine-tuning Large Language Models (LLMs). It includes components for tokenization, sharding, and embedding generation, optimized for handling large-scale data efficiently.
It also generates cosine similarities between words via their embeddings.

## Components

1. **ShardingText**: Splits input text files into multiple shards while preserving line numbers.
2. **FrequencyMR**: MapReduce job for counting the number of occurences of words in the text corpus.
3. **TokenizerMR**: A MapReduce job for tokenizing text data in a distributed manner.
3. **EmbeddingsHelper**: Processes tokenized data, merges tokens, and creates input shards for embedding generation.
4. **SimilarityMR**: A MapReduce job for calculating token similarities based on embeddings.

## Setup

### Prerequisites

- Scala (version 2.12)
- Apache Hadoop (version 3.3.6)
- SBT (Scala Build Tool, version 1.10.2)
- Java JDK (version 1.8 or higher)

### Configuration

1. Clone the repository:
   ```
   git clone [repository-url]
   cd [project-directory]
   ```

2. Set up your Hadoop environment variables:
   ```
   export HADOOP_CLASSPATH=$(hadoop classpath)
   ```

3. Configure `application.conf` with appropriate settings:
   ```
   myapp {
    numShards = 20
    windowSize = 8
    stride = 4
    numShardsEmbeddings = 500
    vocabSize = 252975
    epoch = 10
    embeddingDim = 10
    initialLearningRate = 0.01
    decayRate = 0.96
    decaySteps = 1000
    patience = 10
   }
   ```

## Usage

### 1. Build Jar

```bash
sbt clean assembly
```

### 2. Run Jar in Hadoop

```bash
hadoop jar <jar_location> main /input /word_count /tokenizer_input /tokenizer_output /merged_tokens /embeddings_input /embeddings_output /merged_embeddings /output
```

## Data Processing Flow

1. Word Frequency is calculated using `WordFrequencyMR` MapReduce job.
2. Input text files are sharded using `ShardingText`.
3. Sharded text is tokenized using the `TokenizerMR` MapReduce job.
4. Tokenized data is processed and merged using `EmbeddingsHelper`.
5. Token similarities are calculated using the `SimilarityMR` MapReduce job.

## Important Considerations

- Recommended settings are:
    - Window Size: 8 tokens
    - Stride: 2 tokens
- These settings can be adjusted based on specific needs and model architecture.
- The project is optimized for handling large datasets efficiently, using streaming processes to minimize memory usage.

## Troubleshooting

- If encountering `OutOfMemoryError`, try increasing the JVM heap size:
  ```
  export JAVA_OPTS="-Xmx4g"
  ```
- Ensure Hadoop is properly configured and HDFS paths are correct.

## Contact

Email : asinh40@uic.edu