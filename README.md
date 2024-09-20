Overview

This system integrates real-time stock price data fetching, streaming, prediction, and visualization into a seamless workflow using Python, Kafka, Docker, and Streamlit. The project is structured around four main Python scripts and a Docker setup to ensure ease of deployment and scalability.

System Architecture

Data Fetching: streaming_kafka.py fetches real-time stock data using external APIs and sends it to Kafka for streaming.
Data Streaming: Kafka acts as a central hub for data streaming, ensuring that data flows efficiently between components.
Data Prediction: prediction.py subscribes to the Kafka topic to fetch data and applies predictive models to forecast stock prices.
Interactive Dashboards: dashboard.py utilizes Streamlit to create web interfaces that display real-time insights and predictions.
Data Visualization: indicator_test.py accesses both real-time and predicted data to generate graphical representations.
Each component is encapsulated in Docker containers, orchestrated with Docker Compose to streamline deployment and scaling.

Installation

Prerequisites
Python 3.8+
Docker and Docker Compose
Apache Kafka

Troubleshooting

Kafka Connection Issues: Ensure Kafka and Zookeeper services are running and accessible.
Data Fetching Errors: Check API limits and connectivity.
Streamlit Interface Not Loading: Verify that Streamlit is properly installed in the Docker container and ports are mapped correctly.

***StockBot***
Core Features
URL Loading: Users can input URLs directly or upload text files containing URLs to fetch the content of news articles.
Content Processing: The tool leverages LangChain's UnstructuredURL Loader to process the fetched article content.
Data Embedding and Indexing: It uses OpenAI's embeddings to convert article content into vector forms and employs FAISS (Facebook AI Similarity Search), a library for efficient similarity searching, to index these vectors. This setup enhances the speed and relevance of information retrieval.
Interaction with AI: Users can interact with the tool through a chat interface (likely powered by ChatGPT) to ask questions and receive concise answers along with the source URLs.
Technologies Used
OpenAI Embeddings: These are used to convert text into numerical vectors that represent the semantic content of the texts.
FAISS: An efficient similarity search and clustering of dense vectors. It allows quick retrieval of items that are similar to a queried item.
LangChain: A tool for loading and processing unstructured text data from URLs.
Streamlit: An open-source app framework used primarily for machine learning and data science projects. It is used here to create the web interface for StockBot.