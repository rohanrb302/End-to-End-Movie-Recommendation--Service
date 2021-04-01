# Movie-Recommendation-System


This was the final project for ML for Production Systesms, it involves building an end to end Movie recommendaiton system.

The work was done in a team of 4. The final presenation delves into some of the details.

The pipeline invloved extracting data from a live Kafka stream, cleaning and transforming finally storing the data to PostgreSQL database(RDS). We built a Neural Network model for the recommender system. Folllowing A-B testing of a model, it  would be automatically deployed into the production setting using Kubernetes(AKS). We also built a simple load balancer in Go. The project also involved monitoring the models in production using Grafana.  
