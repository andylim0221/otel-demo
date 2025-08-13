for i in {1..20}; do
  echo "Making Auto Instrumentation HTTP call $i"
  curl -f -LI http://localhost:5000/aws-sdk-call-auto-instrumentation 
done

for i in {1..20}; do
  echo "Making Manual Instrumentation HTTP call $i"
  curl -f -LI http://localhost:5003/aws-sdk-call-manual-instrumentation 
done
