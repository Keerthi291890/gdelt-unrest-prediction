# gdelt-unrest-prediction
# Real-Time Political Unrest Prediction from Global News Signals

A Spark-based pipeline that estimates political unrest risk from GDELT's global news stream, updated every 15 minutes. Escalating unrest shows up in shifting conflict-event share and emotional tone before it's obvious in any single headline, so these signals become an early-warning sensor. The pipeline turns raw events and GKG sentiment into live per-country escalation forecasts, with an MLlib classifier trained on history and updated as new data arrives. Results are benchmarked against a published country-instability study to test whether richer sentiment and real-time delivery improve on a static baseline.

## Status

🚧 Early development — architecture and design docs are in place; pipeline implementation in progress.

## Course context

Built for the Big Data Analytics course (Hadoop, Scala/FP, Apache Spark, MLlib).

