#!/bin/bash

source ./docker/names.config

# MasterAddr has to be of the host, watchout for WSL
if [[ $# -eq 1 ]] && [[ $1 -eq "-i" ]]; then
    docker run --rm -it -p 55557:55557/tcp -e MasterAddr=192.168.56.1 -e HostAddr=192.168.56.1 --name=$WORKER_CONTAINER_NAME  $WORKER_IMAGE_NAME:latest
    exit 0
fi

re='^[0-9]+$'

if [[ $# -eq 1 ]] && [[ $1 =~ $re ]]; then
    for i in $(seq 1 1 $1); do
        docker run --rm -d -p 55557/tcp $WORKER_IMAGE_NAME:latest
    done
else
    docker run --rm -d -p 55557/tcp $WORKER_IMAGE_NAME:latest
fi

