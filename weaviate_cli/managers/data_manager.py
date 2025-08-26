import click
import json
import numpy as np
import random
import os
from importlib import resources
from weaviate_cli.utils import get_random_string, pp_objects
from weaviate import WeaviateClient
from weaviate.classes.query import MetadataQuery
from weaviate.collections.classes.tenants import TenantActivityStatus
from typing import Dict, List, Optional, Union, Any, Tuple, Callable
import weaviate.classes.config as wvc
from weaviate.classes.query import Filter
from weaviate.collections import Collection
from datetime import datetime, timedelta
from weaviate_cli.defaults import (
    MAX_OBJECTS_PER_BATCH,
    QUERY_MAXIMUM_RESULTS,
    MAX_WORKERS,
    CreateCollectionDefaults,
    CreateDataDefaults,
    CreateTenantsDefaults,
    QueryDataDefaults,
    UpdateDataDefaults,
    DeleteDataDefaults,
)
import importlib.resources as resources
from pathlib import Path
import math
from faker import Faker
import time

PROPERTY_NAME_MAPPING = {
    "releaseDate": "release_date",
    "originalLanguage": "original_language",
    "productionCountries": "production_countries",
    "spokenLanguages": "spoken_languages",
}

# Constants for data generation optimization
# Use more than CPU count to account for I/O-bound tasks

# Define movie-related data outside the class to be accessible by worker processes
MOVIE_GENRES = [
    "Action",
    "Adventure",
    "Animation",
    "Comedy",
    "Crime",
    "Documentary",
    "Drama",
    "Family",
    "Fantasy",
    "History",
    "Horror",
    "Music",
    "Mystery",
    "Romance",
    "Science Fiction",
    "Thriller",
    "War",
    "Western",
]

MOVIE_KEYWORDS = [
    "love",
    "revenge",
    "suspense",
    "thriller",
    "hero",
    "villain",
    "journey",
    "quest",
    "discovery",
    "betrayal",
    "friendship",
    "family",
    "war",
    "peace",
    "survival",
    "aliens",
    "future",
    "past",
    "magic",
    "technology",
    "robots",
    "artificial intelligence",
    "space",
    "ocean",
    "mountains",
    "city",
    "rural",
    "coming of age",
    "disaster",
    "conspiracy",
    "time travel",
    "dystopia",
    "utopia",
    "zombies",
    "vampires",
    "werewolves",
    "ghosts",
    "haunted",
    "musical",
    "comedy",
    "tragedy",
    "redemption",
    "sacrifice",
    "politics",
    "religion",
    "heist",
    "chase",
]

LANGUAGES = [
    {"iso_639_1": "en", "name": "English"},
    {"iso_639_1": "es", "name": "Spanish"},
    {"iso_639_1": "fr", "name": "French"},
    {"iso_639_1": "de", "name": "German"},
    {"iso_639_1": "it", "name": "Italian"},
    {"iso_639_1": "ja", "name": "Japanese"},
    {"iso_639_1": "ko", "name": "Korean"},
    {"iso_639_1": "zh", "name": "Chinese"},
    {"iso_639_1": "hi", "name": "Hindi"},
    {"iso_639_1": "ru", "name": "Russian"},
    {"iso_639_1": "pt", "name": "Portuguese"},
    {"iso_639_1": "ar", "name": "Arabic"},
]

COUNTRIES = [
    {"iso_3166_1": "US", "name": "United States of America"},
    {"iso_3166_1": "GB", "name": "United Kingdom"},
    {"iso_3166_1": "CA", "name": "Canada"},
    {"iso_3166_1": "AU", "name": "Australia"},
    {"iso_3166_1": "FR", "name": "France"},
    {"iso_3166_1": "DE", "name": "Germany"},
    {"iso_3166_1": "IT", "name": "Italy"},
    {"iso_3166_1": "JP", "name": "Japan"},
    {"iso_3166_1": "KR", "name": "South Korea"},
    {"iso_3166_1": "CN", "name": "China"},
    {"iso_3166_1": "IN", "name": "India"},
    {"iso_3166_1": "BR", "name": "Brazil"},
    {"iso_3166_1": "MX", "name": "Mexico"},
]

STATUSES = ["Released", "In Production", "Post Production", "Planned", "Cancelled"]


# Standalone function for parallel processing
def generate_movie_object(is_update: bool = False, seed: Optional[int] = None) -> Dict:
    """Generate a single movie object - standalone function for parallel processing"""
    # Set seed if provided for deterministic generation
    if seed is not None:
        random.seed(seed)
        fake = Faker()
        fake.seed_instance(seed)
    else:
        fake = Faker()

    # Generate a realistic release date
    date = datetime.now() - timedelta(
        days=random.randint(0, 20 * 365)
    )  # Random date in last 20 years
    release_date = date.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Select random language codes for spoken languages
    num_spoken_languages = random.randint(1, 3)
    spoken_languages = random.sample(LANGUAGES, num_spoken_languages)

    # Select random countries for production
    num_production_countries = random.randint(1, 3)
    production_countries = random.sample(COUNTRIES, num_production_countries)

    # Select a random original language
    original_language = random.choice(LANGUAGES)

    # Generate random movie keywords
    num_keywords = random.randint(3, 8)
    keywords = " ".join(random.sample(MOVIE_KEYWORDS, num_keywords))

    # Select random genres
    num_genres = random.randint(1, 4)
    genres = " ".join(random.sample(MOVIE_GENRES, num_genres))

    # Generate a movie title with proper capitalization
    title = fake.catch_phrase()

    # Prefix for update operations
    prefix = "updated-" if is_update else ""

    return {
        "title": f"{prefix}{title}",
        "genres": f"{prefix}{genres}",
        "keywords": f"{prefix}{keywords}",
        "director": f"{prefix}{fake.name()}",
        "popularity": float(random.randint(1, 200)),
        "runtime": str(random.randint(70, 210)),  # Movie runtime in minutes
        "cast": f"{prefix}{fake.name()} {fake.name()} {fake.name()}",
        "originalLanguage": original_language["iso_639_1"],
        "tagline": f"{prefix}{fake.sentence()}",
        "budget": random.randint(1_000_000, 250_000_000),
        "releaseDate": release_date,
        "revenue": random.randint(1_000_000, 2_000_000_000),
        "status": random.choice(STATUSES),
        "spokenLanguages": spoken_languages,
        "productionCountries": production_countries,
    }


# Standalone small-chunk generator for streaming + multiprocessing
# Generates a list[dict] using generate_movie_object; args: (chunk_size, base_seed, start_index, is_update)
def _streaming_generate_chunk(args) -> List[Dict]:
    chunk_size, base_seed, start_index, is_update = args
    results: List[Dict] = []
    for i in range(chunk_size):
        seed = (base_seed + start_index + i) if base_seed is not None else None
        results.append(generate_movie_object(is_update, seed))
    return results


class DataManager:
    def __init__(self, client: WeaviateClient):
        self.client = client
        self.fake = Faker()
        # Seed the Faker instance for reproducibility
        Faker.seed(42)

    def __producer_consumer_ingest(
        self,
        collection: Collection,
        num_objects: int,
        vectorizer: str,
        vector_dimensions: int,
        named_vectors: Optional[List[str]],
        uuid: Optional[str],
        dynamic_batch: bool,
        batch_size: int,
        concurrent_requests: int,
        multi_vector: bool,
        skip_seed: bool,
        verbose: bool,
    ) -> Tuple[int, List]:
        """Memory-safe producer→queue ingestion with two clear modes:
        - dynamic_batch=True: Fast streaming generation via multiprocessing feeding a single dynamic batcher.
        - dynamic_batch=False: Modest thread producers + single fixed_size batcher (which handles HTTP concurrency).
        """
        from queue import Queue, Empty
        import threading

        failed_objects: List = []

        # Shared helper: build vector payload according to vectorizer configuration
        def build_vector() -> (
            Optional[
                Union[List[float], Dict[str, List[float]], Dict[str, List[List[float]]]]
            ]
        ):
            if vectorizer != "none":
                return None
            if multi_vector and named_vectors and len(named_vectors) > 0:
                return {
                    named_vectors[0]: [
                        (2 * np.random.rand(vector_dimensions) - 1).tolist(),
                        (2 * np.random.rand(vector_dimensions) - 1).tolist(),
                    ]
                }
            if named_vectors is None:
                return (2 * np.random.rand(vector_dimensions) - 1).tolist()
            return {
                name: (2 * np.random.rand(vector_dimensions) - 1).tolist()
                for name in named_vectors
            }

        # Use deterministic base seed if requested
        base_seed: Optional[int] = 42 if skip_seed else None

        # --- Dynamic mode: multiprocessing producer → single dynamic batch consumer ---
        if dynamic_batch:
            import multiprocessing as mp

            # Tunables for streaming generation
            gen_chunk_size = max(2000, batch_size * 10)
            max_prefetch_chunks = 4
            producer_processes = concurrent_requests
            if verbose:
                print(
                    f"Dynamic mode: streaming with {producer_processes} generator processes, chunk_size={gen_chunk_size}, prefetch={max_prefetch_chunks}"
                )

            # Bounded queue of property-chunks to keep memory in check
            q: Queue[Optional[List[Dict]]] = Queue(maxsize=max_prefetch_chunks)
            consumed = 0

            # Prepare generation tasks
            num_chunks = math.ceil(num_objects / gen_chunk_size)
            task_args: List[Tuple[int, Optional[int], int, bool]] = []
            for chunk_idx in range(num_chunks):
                start_index = chunk_idx * gen_chunk_size
                size = min(gen_chunk_size, num_objects - start_index)
                if size > 0:
                    task_args.append((size, base_seed, start_index, False))

            def feeder() -> None:
                with mp.Pool(processes=producer_processes) as pool:
                    for chunk in pool.imap_unordered(
                        _streaming_generate_chunk, task_args
                    ):
                        q.put(chunk)  # Blocks when buffer full -> back-pressure
                # Signal completion
                q.put(None)

            def consumer() -> None:
                nonlocal consumed
                start_time = time.time()
                last_log = start_time
                with collection.batch.dynamic() as batch:
                    while True:
                        try:
                            chunk = q.get(timeout=0.5)
                        except Empty:
                            continue
                        if chunk is None:
                            break
                        for item in chunk:
                            vec = build_vector()
                            if vec is None:
                                batch.add_object(properties=item, uuid=uuid)
                            else:
                                batch.add_object(properties=item, uuid=uuid, vector=vec)
                            consumed += 1
                        if verbose and time.time() - last_log >= 2.0:
                            elapsed = time.time() - start_time
                            qps = consumed / elapsed if elapsed > 0 else 0
                            print(
                                f"Submitted {consumed}/{num_objects} (~{qps:.0f} obj/s), chunks_in_queue={q.qsize()}"
                            )
                            last_log = time.time()

            # Run feeder and consumer
            feeder_t = threading.Thread(target=feeder, daemon=True)
            consumer_t = threading.Thread(target=consumer, daemon=True)
            feeder_t.start()
            consumer_t.start()
            feeder_t.join()
            consumer_t.join()

            # Best-effort failure reporting (exposed on collection.batch)
            if getattr(collection.batch, "failed_objects", None):
                failed_objects.extend(collection.batch.failed_objects)

            return consumed, failed_objects

        # --- Fixed-size mode: thread producers → single fixed_size batch consumer ---
        # The batcher manages HTTP concurrency via concurrent_requests; a single consumer is optimal here
        q: Queue[Optional[Dict]] = Queue(maxsize=max(1, batch_size * 20))
        produced = 0
        consumed = 0

        # Modest producer parallelism to avoid GIL contention during property generation
        producer_threads = min(4, max(1, num_objects // max(1, batch_size * 10)))
        if verbose:
            print(
                f"Fixed-size mode: {producer_threads} producers, 1 consumer; batch_size={batch_size}, concurrent_requests={concurrent_requests}"
            )

        # Partition the work across producers
        ranges: List[Tuple[int, int]] = []
        base = num_objects // producer_threads
        remainder = num_objects % producer_threads
        start_idx = 0
        for i in range(producer_threads):
            end_idx = start_idx + base + (1 if i < remainder else 0)
            if end_idx > start_idx:
                ranges.append((start_idx, end_idx))
            start_idx = end_idx

        def producer(lo: int, hi: int, seed_offset: int) -> None:
            nonlocal produced
            for i in range(lo, hi):
                seed = (base_seed + seed_offset + i) if base_seed is not None else None
                item = self.__generate_single_object(is_update=False, seed=seed)
                q.put(item)
                produced += 1

        def consumer_fixed() -> None:
            nonlocal consumed
            start_time = time.time()
            last_log = start_time
            with collection.batch.fixed_size(
                batch_size=batch_size, concurrent_requests=max(1, concurrent_requests)
            ) as batch:
                while True:
                    try:
                        item = q.get(timeout=0.25)
                    except Empty:
                        if all(not t.is_alive() for t in prod_threads) and q.empty():
                            return
                        continue
                    vec = build_vector()
                    if vec is None:
                        batch.add_object(properties=item, uuid=uuid)
                    else:
                        batch.add_object(properties=item, uuid=uuid, vector=vec)
                    consumed += 1
                    if verbose and time.time() - last_log >= 2.0:
                        elapsed = time.time() - start_time
                        qps = consumed / elapsed if elapsed > 0 else 0
                        print(
                            f"Submitted {consumed}/{num_objects} (~{qps:.0f} obj/s), queue={q.qsize()}"
                        )
                        last_log = time.time()

        # Start producers and consumer
        prod_threads = [
            threading.Thread(target=producer, args=(lo, hi, idx * 1000), daemon=True)
            for idx, (lo, hi) in enumerate(ranges)
        ]
        for t in prod_threads:
            t.start()
        cons_thread = threading.Thread(target=consumer_fixed, daemon=True)
        cons_thread.start()
        for t in prod_threads:
            t.join()
        cons_thread.join()

        if getattr(collection.batch, "failed_objects", None):
            failed_objects.extend(collection.batch.failed_objects)

        return consumed, failed_objects

    def __import_json(
        self,
        collection: Collection,
        file_name: str,
        cl: wvc.ConsistencyLevel,
        num_objects: Optional[int] = None,
    ) -> int:
        counter = 0
        properties: List[wvc.Property] = collection.config.get().properties

        try:
            with (
                resources.files("weaviate_cli.datasets")
                .joinpath(file_name)
                .open("r") as f
            ):
                data = json.load(f)

                cl_collection: Collection = collection.with_consistency_level(cl)
                with cl_collection.batch.dynamic() as batch:
                    for obj in data[:num_objects] if num_objects else data:
                        added_obj = {}
                        for prop in properties:
                            prop_name = PROPERTY_NAME_MAPPING.get(prop.name, prop.name)
                            if prop_name in obj and obj[prop_name] != "":
                                added_obj[prop.name] = self.__convert_property_value(
                                    obj[prop_name], prop.data_type
                                )
                        batch.add_object(properties=added_obj)
                        counter += 1

                if cl_collection.batch.failed_objects:
                    for failed_object in cl_collection.batch.failed_objects:
                        print(
                            f"Failed to add object with UUID {failed_object.original_uuid}: "
                            f"{failed_object.message}"
                        )
                    return -1

                expected: int = len(data[:num_objects]) if num_objects else len(data)
                assert (
                    counter == expected
                ), f"Expected {expected} objects, but added {counter} objects."

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON file: {str(e)}")
            return -1
        except FileNotFoundError as e:
            print(f"Dataset file not found: {str(e)}")
            return -1
        except Exception as e:
            print(f"Unexpected error loading data file: {str(e)}")
            return -1

        print(f"Finished processing {counter} objects.")
        return counter

    def __convert_property_value(self, value: Any, data_type: wvc.DataType) -> Any:
        if data_type == wvc.DataType.NUMBER:
            return float(value)
        elif data_type == wvc.DataType.DATE:
            date = datetime.strptime(value, "%Y-%m-%d")
            return date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return value

    def __generate_single_object(
        self, is_update: bool = False, seed: Optional[int] = None
    ) -> Dict:
        """Method to generate a single object for non-parallel use cases"""
        return generate_movie_object(is_update, seed)

    def __ingest_data(
        self,
        collection: Collection,
        num_objects: int,
        cl: wvc.ConsistencyLevel,
        randomize: bool,
        skip_seed: bool,
        vector_dimensions: Optional[int] = 1536,
        uuid: Optional[str] = None,
        verbose: bool = False,
        multi_vector: bool = False,
        dynamic_batch: bool = False,
        batch_size: int = 100,
        concurrent_requests: int = MAX_WORKERS,
    ) -> Collection:
        if randomize:
            click.echo(f"Generating and ingesting {num_objects} objects")
            start_time = time.time()

            # Determine vectorizer setup
            config = collection.config.get()
            if not config.vectorizer and config.vector_config:
                named_vectors = list(config.vector_config.keys())
                vectorizer = config.vector_config[
                    named_vectors[0]
                ].vectorizer.vectorizer
            elif config.vectorizer:
                vectorizer = config.vectorizer
                named_vectors = None
            else:
                vectorizer = "none"
                named_vectors = None

            cl_collection = collection.with_consistency_level(cl)

            # Single consumer that feeds the batcher; batcher does its own HTTP parallelism
            counter, failed_objects = self.__producer_consumer_ingest(
                collection=cl_collection,
                num_objects=num_objects,
                vectorizer=vectorizer,
                vector_dimensions=vector_dimensions or 1536,
                named_vectors=named_vectors,
                uuid=uuid,
                dynamic_batch=dynamic_batch,
                batch_size=batch_size,
                concurrent_requests=concurrent_requests,
                multi_vector=multi_vector,
                skip_seed=skip_seed,
                verbose=verbose,
            )

            if failed_objects:
                for failed_object in failed_objects:
                    print(
                        f"Failed to add object with UUID {failed_object.original_uuid}: {failed_object.message}"
                    )

            total_elapsed = time.time() - start_time
            print(
                f"Inserted {counter} objects into class '{collection.name}'"
                + (
                    f" in {total_elapsed:.2f} seconds ({counter/total_elapsed:.1f} objects/second)"
                    if verbose
                    else ""
                )
            )
            return cl_collection
        else:
            click.echo(f"Importing {num_objects} objects from Movies dataset")
            num_objects_inserted = self.__import_json(
                collection, "movies.json", cl, num_objects
            )
            print(
                f"Inserted {num_objects_inserted} objects into class '{collection.name}'"
            )
            return collection

    def create_data(
        self,
        collection: Optional[str] = CreateDataDefaults.collection,
        limit: int = CreateDataDefaults.limit,
        consistency_level: str = CreateDataDefaults.consistency_level,
        randomize: bool = CreateDataDefaults.randomize,
        skip_seed: bool = CreateDataDefaults.skip_seed,
        tenant_suffix: str = CreateTenantsDefaults.tenant_suffix,
        auto_tenants: int = CreateDataDefaults.auto_tenants,
        tenants_list: Optional[List[str]] = None,
        vector_dimensions: Optional[int] = CreateDataDefaults.vector_dimensions,
        uuid: Optional[str] = None,
        wait_for_indexing: bool = CreateDataDefaults.wait_for_indexing,
        verbose: bool = CreateDataDefaults.verbose,
        multi_vector: bool = CreateDataDefaults.multi_vector,
        dynamic_batch: bool = CreateDataDefaults.dynamic_batch,
        batch_size: int = CreateDataDefaults.batch_size,
        concurrent_requests: int = MAX_WORKERS,
    ) -> Collection:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command"
            )

        col: Collection = self.client.collections.get(collection)
        mt_enabled = col.config.get().multi_tenancy_config.enabled

        if mt_enabled:
            existing_tenants = [
                name
                for name in col.tenants.get().keys()
                if name.startswith(tenant_suffix)
            ]
            if (
                auto_tenants == CreateDataDefaults.auto_tenants
                and len(existing_tenants) == 0
            ):
                raise Exception(
                    f"No tenants present in class '{col.name}' with suffix '{tenant_suffix}'. Please create tenants using <create tenants> command"
                )
        else:
            if (
                tenants_list is not None
                or auto_tenants != CreateDataDefaults.auto_tenants
            ):
                raise Exception(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Adding data to tenants is not possible."
                )

            existing_tenants = ["None"]

        auto_tenants_activated_enabled = (
            col.config.get().multi_tenancy_config.auto_tenant_activation
        )
        auto_tenants_enabled = (
            col.config.get().multi_tenancy_config.auto_tenant_creation
        )
        if auto_tenants > 0 and auto_tenants_enabled is False:

            raise Exception(
                f"Auto tenant creation is not enabled for class '{col.name}'. Please enable it using <update class> command"
            )

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }
        tenants = existing_tenants
        if auto_tenants > 0:
            if tenants_list is not None:
                raise Exception(
                    f"Either --tenants or --auto_tenants must be provided, not both."
                )
            if existing_tenants == "None":
                tenants = [f"{tenant_suffix}-{i}" for i in range(1, auto_tenants + 1)]
            else:
                if len(existing_tenants) < auto_tenants:
                    tenants += [
                        f"{tenant_suffix}-{i}"
                        for i in range(len(existing_tenants) + 1, auto_tenants + 1)
                    ]
        else:
            if tenants_list is not None:
                tenants = tenants_list

        click.echo(f"Preparing to insert {limit} objects into class '{col.name}'")
        for tenant in tenants:
            if tenant == "None":
                initial_length = len(col)
                collection = self.__ingest_data(
                    collection=col,
                    num_objects=limit,
                    cl=cl_map[consistency_level],
                    randomize=randomize,
                    skip_seed=skip_seed,
                    vector_dimensions=vector_dimensions,
                    uuid=uuid,
                    verbose=verbose,
                    multi_vector=multi_vector,
                    dynamic_batch=dynamic_batch,
                    batch_size=batch_size,
                    concurrent_requests=concurrent_requests,
                )
                after_length = len(col)
            else:
                if not auto_tenants_enabled and not col.tenants.exists(tenant):
                    raise Exception(
                        f"Tenant '{tenant}' does not exist. Please create it using <create tenants> command"
                    )
                if (
                    not auto_tenants_activated_enabled
                    and col.tenants.exists(tenant)
                    and col.tenants.get_by_name(tenant).activity_status
                    != TenantActivityStatus.ACTIVE
                ):
                    raise Exception(
                        f"Tenant '{tenant}' is not active. Please activate it using <update tenants> command"
                    )
                if auto_tenants_enabled and not col.tenants.exists(tenant):
                    initial_length = 0
                else:
                    initial_length = len(col.with_tenant(tenant))
                click.echo(f"Processing objects for tenant '{tenant}'")
                collection = self.__ingest_data(
                    collection=col.with_tenant(tenant),
                    num_objects=limit,
                    cl=cl_map[consistency_level],
                    randomize=randomize,
                    skip_seed=skip_seed,
                    vector_dimensions=vector_dimensions,
                    uuid=uuid,
                    verbose=verbose,
                    multi_vector=multi_vector,
                    dynamic_batch=dynamic_batch,
                    batch_size=batch_size,
                    concurrent_requests=concurrent_requests,
                )
                after_length = len(col.with_tenant(tenant))
            if wait_for_indexing:
                collection.batch.wait_for_vector_indexing()
            if after_length - initial_length != limit:
                click.echo(
                    f"Error occurred while ingesting data for tenant '{tenant}'. Expected number of objects inserted: {limit}. Actual number of objects inserted: {after_length - initial_length}. Double check with weaviate-cli get collection"
                )
        return collection

    def __update_data(
        self,
        collection: Collection,
        num_objects: int,
        cl: wvc.ConsistencyLevel,
        randomize: bool,
        skip_seed: bool,
        verbose: bool = False,
    ) -> int:
        """Update objects in the collection, either with random data or incremental changes."""

        if not skip_seed:
            # Set a seed for reproducible random sampling
            random.seed(42)

        start_time = time.time()
        cl_collection = collection.with_consistency_level(cl)
        total_updated = 0

        # Get total collection size
        collection_size = len(collection)
        if verbose:
            print(f"Collection '{collection.name}' contains {collection_size} objects")

        # Determine if we need random offsets (partial update) or sequential (full update)
        use_random_offsets = num_objects < collection_size

        if use_random_offsets and verbose:
            print(
                f"Updating a random subset of {num_objects} objects from a total of {collection_size}"
            )

        # Generate a single random starting offset for the entire update operation if needed
        random_offset = None
        if use_random_offsets:
            # Set a consistent seed for reproducibility
            # Calculate the maximum safe starting offset
            # the client has problem with large offsets, so we limit it to 100000
            max_start_offset = (
                collection_size - num_objects
                if (collection_size - num_objects) < QUERY_MAXIMUM_RESULTS
                else QUERY_MAXIMUM_RESULTS
            )
            if max_start_offset < 0:
                # If we're requesting more objects than exist, adjust
                num_objects = collection_size
                random_offset = 0
            else:
                # Generate a random starting point
                random_offset = random.randint(0, max_start_offset)
            if verbose:
                print(f"Using random offset {random_offset} for update operation")

        # Process in batches to avoid GRPC message size limits
        iterations = math.ceil(num_objects / MAX_OBJECTS_PER_BATCH)

        # Determine vector dimensions for random vector generation
        vector_dimensions = 1536
        # Retrieve the vector size from the first object in the collection
        object_with_vector = collection.query.fetch_objects(
            limit=1, include_vector=True
        )
        if len(object_with_vector.objects) > 0:
            vector_dimensions = len(
                object_with_vector.objects[0].vector["default"]
                if "default" in object_with_vector.objects[0].vector
                else object_with_vector.objects[0].vector
            )

        for i in range(iterations):
            # Determine how many objects to fetch in this batch
            batch_size = min(MAX_OBJECTS_PER_BATCH, num_objects - total_updated)
            if batch_size <= 0:
                break

            # Calculate the offset for this batch
            if use_random_offsets:
                # For random updates, use the random offset plus any additional offset for subsequent batches
                offset = random_offset + (i * MAX_OBJECTS_PER_BATCH)
                if verbose:
                    print(
                        f"Fetching batch {i+1}/{iterations} ({batch_size} objects, offset: {offset})"
                    )
            else:
                # For sequential updates (full collection), just use sequential offsets
                offset = i * batch_size
                if verbose:
                    print(
                        f"Fetching batch {i+1}/{iterations} ({batch_size} objects, offset: {offset})"
                    )

            # Fetch current batch of objects - one simple API call
            res = collection.query.fetch_objects(limit=batch_size, offset=offset)
            data_objects = res.objects

            # Check if we got any objects
            if not data_objects:
                if total_updated == 0:
                    print(
                        f"No objects found in class '{collection.name}'. Insert objects first using <create data> command"
                    )
                    return -1
                else:
                    print(
                        f"No more objects found in class '{collection.name}' after updating {total_updated} objects."
                    )
                    break

            batch_count = len(data_objects)

            # Process this batch based on strategy (random or incremental)
            if randomize:
                if i == 0 and verbose:
                    print(f"Updating objects with random data...")

                random_objects = [
                    self.__generate_single_object(is_update=True)
                    for _ in range(batch_count)
                ]

                # Process each update in current batch
                for j, (obj, updated_props) in enumerate(
                    zip(data_objects, random_objects)
                ):
                    # Generate a random vector (not named)
                    random_vector = np.random.rand(vector_dimensions).tolist()

                    # Replace the object
                    cl_collection.data.replace(
                        uuid=obj.uuid,
                        properties=updated_props,
                        vector=random_vector,
                    )
                    total_updated += 1

                    # Report progress at intervals
                    if verbose and (j + 1) % max(1, batch_count // 5) == 0:
                        batch_progress = (j + 1) / batch_count * 100
                        total_progress = total_updated / num_objects * 100
                        elapsed = time.time() - start_time
                        rate = total_updated / elapsed if elapsed > 0 else 0
                        print(
                            f"Batch progress: {batch_progress:.1f}%, Overall: {total_progress:.1f}% ({total_updated}/{num_objects}), speed: {rate:.1f} objects/second"
                        )
            else:
                # For non-random updates
                if i == 0 and verbose:
                    print(f"Updating objects with incremental changes...")

                for j, obj in enumerate(data_objects):
                    # Update existing object properties
                    for property, value in obj.properties.items():
                        if isinstance(value, str):
                            obj.properties[property] = "updated-" + value
                        elif isinstance(value, int):
                            obj.properties[property] += 1
                        elif isinstance(value, float):
                            obj.properties[property] += 1.0
                        elif isinstance(value, datetime):
                            obj.properties[property] = value + timedelta(days=1)

                    # Update the object
                    cl_collection.data.update(
                        uuid=obj.uuid,
                        properties=obj.properties,
                    )
                    total_updated += 1

                    # Report progress at intervals
                    if verbose and (j + 1) % max(1, batch_count // 5) == 0:
                        batch_progress = (j + 1) / batch_count * 100
                        total_progress = total_updated / num_objects * 100
                        elapsed = time.time() - start_time
                        rate = total_updated / elapsed if elapsed > 0 else 0
                        print(
                            f"Batch progress: {batch_progress:.1f}%, Overall: {total_progress:.1f}% ({total_updated}/{num_objects}), speed: {rate:.1f} objects/second"
                        )

            # If we've processed fewer objects than the batch size, there might not be any more objects
            if not use_random_offsets and batch_count < batch_size:
                break

        # Report completion
        if total_updated < num_objects:
            print(
                f"Warning: Only found {total_updated} objects to update, less than the requested {num_objects}"
            )

        total_elapsed = time.time() - start_time
        print(
            f"Updated {total_updated} objects in class '{collection.name}'"
            + (
                f" in {total_elapsed:.2f} seconds ({total_updated/total_elapsed:.1f} objects/second)"
                if verbose
                else ""
            )
        )

        return total_updated

    def update_data(
        self,
        collection: str = UpdateDataDefaults.collection,
        limit: int = UpdateDataDefaults.limit,
        consistency_level: str = UpdateDataDefaults.consistency_level,
        randomize: bool = UpdateDataDefaults.randomize,
        skip_seed: bool = UpdateDataDefaults.skip_seed,
        verbose: bool = UpdateDataDefaults.verbose,
    ) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using ./create_class.py"
            )

        col: Collection = self.client.collections.get(collection)
        try:
            tenants = [key for key in col.tenants.get().keys()]
        except Exception as e:
            # Check if the error is due to multi-tenancy being disabled
            if "multi-tenancy is not enabled" in str(e):
                click.echo(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Skipping tenant information collection."
                )
                tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        click.echo(f"Preparing to update {limit} objects into class '{col.name}'")
        for tenant in tenants:
            if tenant == "None":
                ret = self.__update_data(
                    col,
                    limit,
                    cl_map[consistency_level],
                    randomize,
                    skip_seed,
                    verbose,
                )
            else:
                click.echo(f"Processing tenant '{tenant}'")
                ret = self.__update_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    randomize,
                    skip_seed,
                    verbose,
                )
            if ret == -1:

                raise Exception(
                    f"Failed to update objects in class '{col.name}' for tenant '{tenant}'"
                )

    def __delete_data(
        self,
        collection: Collection,
        num_objects: int,
        cl: wvc.ConsistencyLevel,
        uuid: Optional[str] = None,
        verbose: bool = False,
    ) -> int:

        if uuid:
            start_time = time.time()
            collection.with_consistency_level(cl).data.delete_by_id(uuid=uuid)
            elapsed = time.time() - start_time
            print(
                f"Object deleted: {uuid} from class '{collection.name}'"
                + (f" in {elapsed:.2f} seconds" if verbose else "")
            )
            return 1

        # Calculate the number of full batches and handle any remaining objects
        # Use math.ceil to ensure we process all objects, even if num_objects < MAX_OBJECTS_PER_BATCH
        start_time = time.time()
        iterations = math.ceil(num_objects / MAX_OBJECTS_PER_BATCH)
        deleted_objects = 0

        if verbose:
            print(
                f"Preparing to delete up to {num_objects} objects from class '{collection.name}'"
            )

        for i in range(iterations):
            # Determine how many objects to fetch in this batch
            batch_size = min(MAX_OBJECTS_PER_BATCH, num_objects - deleted_objects)
            if batch_size <= 0:
                break

            if verbose:
                print(f"Fetching batch {i+1}/{iterations} ({batch_size} objects)")

            res = collection.query.fetch_objects(limit=batch_size)
            if len(res.objects) == 0:
                print(
                    f"No objects found in class '{collection.name}'. Insert objects first using <create data> command"
                )
                return deleted_objects

            ids = [o.uuid for o in res.objects]
            batch_start = time.time()
            collection.with_consistency_level(cl).data.delete_many(
                where=Filter.by_id().contains_any(ids)
            )
            batch_elapsed = time.time() - batch_start

            deleted_objects += len(ids)

            # Progress reporting if verbose
            if verbose:
                progress = min(100, (deleted_objects / num_objects) * 100)
                elapsed = time.time() - start_time
                rate = deleted_objects / elapsed if elapsed > 0 else 0
                print(
                    f"Progress: {progress:.1f}% ({deleted_objects}/{num_objects}), "
                    + f"batch of {len(ids)} deleted in {batch_elapsed:.2f}s (rate: {rate:.1f} objects/second)"
                )

            # If we've deleted fewer objects than expected, there might not be any more objects
            if len(ids) < batch_size:
                break

        total_elapsed = time.time() - start_time
        print(
            f"Deleted {deleted_objects} objects from class '{collection.name}'"
            + (
                f" in {total_elapsed:.2f} seconds ({deleted_objects/total_elapsed:.1f} objects/second)"
                if verbose
                else ""
            )
        )
        return deleted_objects

    def delete_data(
        self,
        collection: str = DeleteDataDefaults.collection,
        limit: int = DeleteDataDefaults.limit,
        consistency_level: str = DeleteDataDefaults.consistency_level,
        tenants_list: Optional[List[str]] = None,
        uuid: Optional[str] = DeleteDataDefaults.uuid,
        verbose: bool = DeleteDataDefaults.verbose,
    ) -> None:

        if not self.client.collections.exists(collection):
            print(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command."
            )

            return 1

        col: Collection = self.client.collections.get(collection)
        mt_enabled = col.config.get().multi_tenancy_config.enabled
        if mt_enabled:
            existing_tenants = [key for key in col.tenants.get().keys()]
        else:
            existing_tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        if tenants_list is not None:
            tenants = tenants_list
        else:
            tenants = existing_tenants

        for tenant in tenants:
            if tenant == "None":
                ret = self.__update_data(
                    col, limit, cl_map[consistency_level], uuid, verbose
                )
            else:
                click.echo(f"Processing tenant '{tenant}'")
                ret = self.__update_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    uuid,
                    verbose,
                )
            if ret == -1:

                raise Exception(
                    f"Failed to delete objects in class '{col.name}' for tenant '{tenant}'"
                )

    def __query_data(
        self,
        collection: Collection,
        num_objects: int,
        cl: wvc.ConsistencyLevel,
        search_type: str,
        query: str,
        properties: str,
        target_vector: Optional[str] = None,
    ) -> None:

        start_time = datetime.now()
        response = None
        if search_type == "fetch":
            # Fetch logic
            response = collection.with_consistency_level(cl).query.fetch_objects(
                limit=num_objects
            )
        elif search_type == "vector":
            # Vector logic
            response = collection.with_consistency_level(cl).query.near_text(
                query=query,
                return_metadata=MetadataQuery(distance=True, certainty=True),
                limit=num_objects,
                target_vector=target_vector,
            )
        elif search_type == "keyword":
            # Keyword logic
            response = collection.with_consistency_level(cl).query.bm25(
                query=query,
                return_objects=True,
                return_metadata=MetadataQuery(score=True, explain_score=True),
                limit=num_objects,
            )
        elif search_type == "hybrid":
            # Hybrid logic
            response = collection.with_consistency_level(cl).query.hybrid(
                query=query,
                return_metadata=MetadataQuery(score=True),
                limit=num_objects,
                target_vector=target_vector,
            )
        elif search_type == "uuid":
            # UUID logic
            num_objects = 1
            response = collection.with_consistency_level(cl).query.fetch_object_by_id(
                uuid=query
            )

        else:
            click.echo(
                f"Invalid search type: {search_type}. Please choose from 'fetch', 'vector', 'keyword', or 'hybrid'."
            )
            return -1

        if response is not None:
            properties_list = [prop.strip() for prop in properties.split(",")]
            pp_objects(response, properties_list)
        else:
            click.echo("No objects found")
            return -1
        end_time = datetime.now()
        latency = end_time - start_time

        print(
            f"Queried {num_objects} objects using {search_type} search into class '{collection.name}' in {latency.total_seconds()} s"
        )
        return num_objects

    def query_data(
        self,
        collection: str = QueryDataDefaults.collection,
        search_type: str = QueryDataDefaults.search_type,
        query: str = QueryDataDefaults.query,
        consistency_level: str = QueryDataDefaults.consistency_level,
        limit: int = QueryDataDefaults.limit,
        properties: str = QueryDataDefaults.properties,
        tenants: Optional[str] = QueryDataDefaults.tenants,
        target_vector: Optional[str] = QueryDataDefaults.target_vector,
    ) -> None:

        if not self.client.collections.exists(collection):

            raise Exception(
                f"Class '{collection}' does not exist in Weaviate. Create first using <create class> command."
            )

        col: Collection = self.client.collections.get(collection)
        mt_enabled = col.config.get().multi_tenancy_config.enabled
        if mt_enabled:
            if tenants is not None:
                tenants_with_status = col.tenants.get_by_names(tenants.split(","))
                existing_tenants = [
                    tenant
                    for tenant, status in tenants_with_status.items()
                    if status.activity_status == TenantActivityStatus.ACTIVE
                ]
            else:
                existing_tenants = [
                    key
                    for key, tenant in col.tenants.get().keys()
                    if tenant.activity_status == TenantActivityStatus.ACTIVE
                ]
        else:
            if tenants is not None:
                raise Exception(
                    f"Collection '{col.name}' does not have multi-tenancy enabled. Querying tenants is not possible."
                )
            existing_tenants = ["None"]

        cl_map = {
            "quorum": wvc.ConsistencyLevel.QUORUM,
            "all": wvc.ConsistencyLevel.ALL,
            "one": wvc.ConsistencyLevel.ONE,
        }

        for tenant in existing_tenants:
            if tenant == "None":
                ret = self.__query_data(
                    col,
                    limit,
                    cl_map[consistency_level],
                    search_type,
                    query,
                    properties,
                    target_vector,
                )
            else:
                print(f"Querying tenant '{tenant}'")
                ret = self.__query_data(
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    search_type,
                    query,
                    properties,
                    target_vector,
                )
            if ret == -1:

                raise Exception(
                    f"Failed to query objects in class '{col.name}' for tenant '{tenant}'"
                )
