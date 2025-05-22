#!/bin/bash

SESSION_NAME="my-port-forwards"

# Create a new tmux session if it doesn't exist
tmux has-session -t $SESSION_NAME 2>/dev/null

if [ $? != 0 ]; then
  tmux new-session -d -s $SESSION_NAME

  tmux send-keys -t $SESSION_NAME "kubectl port-forward -n kubeflow pod/minio-758b66c84b-t6sjh 9000:9000 --address=0.0.0.0" C-m
  tmux split-window -t $SESSION_NAME
  tmux send-keys -t $SESSION_NAME "kubectl port-forward --address 0.0.0.0 svc/keycloak -n default 7999:8080" C-m
  tmux split-window -t $SESSION_NAME
  tmux send-keys -t $SESSION_NAME "kubectl port-forward --address 0.0.0.0 -n devops-tools svc/jenkins-service 7997:8080" C-m
  tmux split-window -t $SESSION_NAME
  tmux send-keys -t $SESSION_NAME "kubectl port-forward --address 0.0.0.0 svc/istio-ingressgateway -n istio-system 8080:80" C-m
  # Continue to add more split-window and send-keys commands for additional port-forwards if needed

  echo "Tmux session '$SESSION_NAME' started with port-forwards."
  echo "Attach with: tmux attach -t $SESSION_NAME"
  echo "To stop: Attach, then Ctrl+C in each pane, or use 'tmux kill-session -t $SESSION_NAME'"
else
  echo "Tmux session '$SESSION_NAME' already exists. Attach with: tmux attach -t $SESSION_NAME"
fi

# Attach to the session (optional, you might just want to start it and detach)
# tmux attach -t $SESSION_NAME