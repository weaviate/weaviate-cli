from semi.config.configuration import Configuration


def ping(cfg:Configuration):
    if (cfg.client.is_ready()):
        print("Weaviate is reachable!")
    else:
        print("Weaviate not reachable!")