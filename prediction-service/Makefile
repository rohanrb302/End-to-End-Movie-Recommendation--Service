clean:
	rm -rf model/recommedations.csv model/mappings.pkl prediction-webserver.yaml prediction-service.tar

build-container:
	pip3 install boto3
	s3_files=$$(sed '3q;d' ./version); \
    cd ../build_utils; python3 download-s3.py netflixsoftware $$s3_files; \
	mv recommendations.csv mappings.pkl ../prediction-service/model	
	tag=$$(sed '2q;d' ./version); \
	docker build -t kartikri/seai-recommendation-system:$$tag -t kartikri/seai-recommendation-system:latest .

push-container:
	docker push kartikri/seai-recommendation-system

deploy-container:
	docker pull kartikri/seai-recommendation-system:latest
	docker save kartikri/seai-recommendation-system:latest > prediction-service.tar
	microk8s ctr images rm docker.io/kartikri/seai-recommendation-system:latest
	microk8s ctr image import prediction-service.tar
	microk8s kubectl kustomize k8s-conf > prediction-webserver.yaml
	microk8s kubectl apply -f prediction-webserver.yaml
