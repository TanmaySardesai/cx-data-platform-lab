# Lab 10 — Kubernetes and operators (Sat Part B, ~60 min)

Install: `kind` (https://kind.sigs.k8s.io), `kubectl`, `helm`. Everything runs in Docker on your laptop, free.
(Stop the lab's docker compose stack first if your laptop is short on memory: `docker compose stop`.)

## Part B1 — core objects (25 min)
```bash
kind create cluster --name cx
kubectl apply -f labs/10_k8s/app.yaml
kubectl get pods -o wide                       # 2 pods from one Deployment
kubectl port-forward svc/cx-metrics-api 8085:80 & curl localhost:8085
kubectl scale deployment cx-metrics-api --replicas=4 && kubectl get pods
kubectl delete pod -l app=cx-metrics-api --wait=false && kubectl get pods -w   # self-healing: new pods appear
kubectl set env deployment/cx-metrics-api RELEASE=v2 && kubectl rollout status deployment/cx-metrics-api   # any pod-spec change triggers a rolling update
kubectl rollout undo deployment/cx-metrics-api
```
Explain in your own words: pod vs deployment vs service; requests vs limits; readiness probe; what the Deployment controller did when you deleted pods (desired vs actual state).

## Part B2 — the Spark Operator (35 min)
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator && helm repo update
helm install spark-operator spark-operator/spark-operator \
  --namespace spark-operator --create-namespace --set "spark.jobNamespaces={default}" --wait
kubectl get crd | grep sparkoperator          # the custom resource types the operator adds
kubectl get sa -n default                     # find the spark service account; match it in spark-pi.yaml
kubectl apply -f labs/10_k8s/spark-pi.yaml
kubectl get sparkapplications -w              # SUBMITTED -> RUNNING -> COMPLETED
kubectl get pods                              # driver + executor pods the operator created for you
kubectl logs spark-pi-driver | grep "Pi is roughly"
```
That's the operator pattern: a custom resource (SparkApplication) + a controller that reconciles it into pods. Same idea as Strimzi (Kafka), the Flink operator, and EMR on EKS's job runs.

Stretch: change `executor.instances` to 4, delete and re-apply, and watch more executor pods.

Clean up: `kind delete cluster --name cx`

## Be ready to explain
Why teams run Spark/Airflow on Kubernetes (shared infra, isolation per job, autoscaling, same tooling) and the cost (operational complexity, you now run the platform). IRSA on EKS: pods get an IAM role through their service account, no keys in containers.
