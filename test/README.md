# Running Tests

## Unit Tests

Unit tests ensure that individual components of the Weaviate CLI are functioning correctly. To run the unit tests, execute the following command:

```bash
pytest test/unittests
```

Unit tests do not require a running Weaviate cluster.

## Integration Tests

Integration tests verify that different components of the system work together as expected. These tests require a local Weaviate instance. To set up and start a local cluster using `weaviate-local-k8s`, follow these steps:

1. **Download the `weaviate-local-k8s` repository:**

   Clone the repository from [weaviate-local-k8s](https://github.com/weaviate/weaviate-local-k8s.git):

   ```bash
   git clone https://github.com/weaviate/weaviate-local-k8s.git
   ```

2. **Start the Weaviate Instance:**

   Navigate to the cloned repository directory and run the setup script:

   ```bash
   WORKERS=1 REPLICAS=1 WEAVIATE_VERSION="1.27.0" MODULES="text2vec-contextionary,text2vec-transformers" ENABLE_BACKUP=true OBSERVABILITY=true ./local-k8s.sh setup
   ```

Once the Weaviate instance is running, execute the integration tests by running:

```bash
pytest tests/integration
```

## GitHub Actions Integration

The tests are automatically triggered by the GitHub Actions workflow defined in `.github/workflows/main.yaml`. This workflow runs the unit tests across multiple Python versions on every push to the `main` branch, on tags, and on pull requests. For more information on setting up GitHub Actions for Python testing, refer to [Building and Testing Python](https://docs.github.com/en/actions/use-cases-and-examples/building-and-testing/building-and-testing-python).
