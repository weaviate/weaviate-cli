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
import multiprocessing
from functools import partial
import concurrent.futures
import time
from concurrent.futures import ThreadPoolExecutor

PROPERTY_NAME_MAPPING = {
    "releaseDate": "release_date",
    "originalLanguage": "original_language",
    "productionCountries": "production_countries",
    "spokenLanguages": "spoken_languages",
}

# Constants for data generation optimization
CHUNK_SIZE = 10000  # Number of objects to generate per chunk/worker
MAX_WORKERS = min(
    32, multiprocessing.cpu_count() + 4
)  # Use more than CPU count to account for I/O-bound tasks

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


def generate_chunk(args) -> List[Dict]:
    """Generate a chunk of movie objects - standalone function for multiprocessing"""
    chunk_size, chunk_id, is_update, skip_seed = args
    # Use different seeds for each worker to ensure                                                  unique random data
    base_seed = 42 + chunk_id * 1000 if not skip_seed else None
    return [
        generate_movie_object(is_update, base_seed + i if base_seed else None)
        for i in range(chunk_size)
    ]


class DataManager:
    def __init__(self, client: WeaviateClient):
        self.client = client
        self.fake = Faker()
        # Seed the Faker instance for reproducibility
        Faker.seed(42)

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

    def __generate_single_object(self, is_update: bool = False) -> Dict:
        """Method to generate a single object for non-parallel use cases"""
        return generate_movie_object(is_update)

    def __generate_data_object(
        self,
        limit: int,
        skip_seed: bool,
        is_update: bool = False,
        verbose: bool = False,
    ) -> Union[List[Dict], Dict]:
        """Generate data objects, using parallel processing for large batches"""

        # For small numbers, don't bother with parallel processing
        if limit <= 1000:
            return [
                self.__generate_single_object(is_update=is_update) for _ in range(limit)
            ]

        # For large numbers, use parallel processing
        start_time = time.time()

        # Calculate chunks
        num_chunks = math.ceil(limit / CHUNK_SIZE)
        chunk_sizes = [CHUNK_SIZE] * (num_chunks - 1)
        # The last chunk might be smaller
        chunk_sizes.append(limit - sum(chunk_sizes))

        # Prepare arguments for worker processes
        task_args = [
            (size, i, is_update, skip_seed)
            for i, size in enumerate(chunk_sizes)
            if size > 0
        ]

        # Use multiprocessing Pool to avoid pickling issues with ProcessPoolExecutor
        results = []
        with multiprocessing.Pool(processes=MAX_WORKERS) as pool:
            # Map the generation function to the chunk arguments
            for i, chunk_result in enumerate(
                pool.imap_unordered(generate_chunk, task_args)
            ):
                results.extend(chunk_result)
                # Report progress periodically
                current_count = len(results)
                progress = current_count / limit * 100
                if verbose and (
                    i % max(1, num_chunks // 10) == 0 or i == len(task_args) - 1
                ):
                    print(
                        f"Data generation progress: {progress:.1f}% ({current_count}/{limit})"
                    )

        elapsed = time.time() - start_time
        if verbose:
            print(
                f"Generated {len(results)} objects in {elapsed:.2f} seconds ({len(results)/elapsed:.0f} objects/second)"
            )

        return results

    def __ingest_batch(
        self,
        batch_objects: List[Dict],
        collection: Collection,
        vectorizer: str,
        vector_dimensions: int,
        named_vectors: Optional[List[str]],
        uuid: Optional[str],
        batch_index: int = 0,
        verbose: bool = False,
    ) -> Tuple[int, List]:
        """Process a single batch of objects for ingestion"""
        counter = 0
        failed_objects = []
        batch_size = len(batch_objects)

        if verbose:
            print(f"Starting batch {batch_index+1} with {batch_size} objects")

        log_interval = max(1, batch_size // 5)  # Log 5 times per batch
        start_time = time.time()

        with collection.batch.dynamic() as batch:
            for i, obj in enumerate(batch_objects):
                if vectorizer == "none":
                    # Generate vector(s) for the object
                    if named_vectors is None:
                        vector = (2 * np.random.rand(vector_dimensions) - 1).tolist()
                    else:
                        vector = {
                            name: (2 * np.random.rand(vector_dimensions) - 1).tolist()
                            for name in named_vectors
                        }
                    batch.add_object(properties=obj, uuid=uuid, vector=vector)
                else:
                    # Let the vectorizer generate the vector
                    batch.add_object(properties=obj, uuid=uuid)
                counter += 1

                # Log progress periodically
                if verbose and (i + 1) % log_interval == 0:
                    progress = (i + 1) / batch_size * 100
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed if elapsed > 0 else 0
                    print(
                        f"Batch {batch_index+1}: {progress:.1f}% ({i+1}/{batch_size}) at {rate:.1f} objects/second"
                    )

            # Collect failed objects
            if collection.batch.failed_objects:
                failed_objects.extend(batch.failed_objects)

        if verbose:
            total_elapsed = time.time() - start_time
            rate = batch_size / total_elapsed if total_elapsed > 0 else 0
            print(
                f"Completed batch {batch_index+1}: {counter} objects in {total_elapsed:.2f}s ({rate:.1f} objects/second)"
            )

        return counter, failed_objects

    def __ingest_data(
        self,
        collection: Collection,
        num_objects: int,
        cl: wvc.ConsistencyLevel,
        randomize: bool,
        skip_seed: bool,
        vector_dimensions: Optional[int] = 1536,
        uuid: Optional[str] = None,
        named_vectors: Optional[List[str]] = None,
        verbose: bool = False,
    ) -> Collection:
        if randomize:
            click.echo(f"Generating {num_objects} objects")
            start_time = time.time()

            # Determine vector dimensions based on vectorizer
            vectorizer = collection.config.get().vectorizer

            # Generate all the data objects
            data_objects = self.__generate_data_object(
                num_objects, skip_seed, verbose=verbose
            )
            if verbose:
                print(f"Data generation complete. Beginning ingestion...")

            # Get collection with consistency level
            cl_collection = collection.with_consistency_level(cl)

            # Determine if parallel processing makes sense based on dataset size
            MIN_OBJECTS_FOR_PARALLEL = (
                1000  # Only use parallel for datasets >= this size
            )
            MIN_BATCH_SIZE = 100  # Minimum batch size for each worker

            # For small datasets, use a single batch
            if num_objects < MIN_OBJECTS_FOR_PARALLEL:
                if verbose:
                    print(
                        f"Processing {num_objects} objects in a single batch (small dataset)"
                    )

                counter, failed_objects = self.__ingest_batch(
                    data_objects,
                    cl_collection,
                    vectorizer,
                    vector_dimensions,
                    named_vectors,
                    uuid,
                    0,
                    verbose,
                )

                # Handle any failed objects
                if failed_objects:
                    for failed_object in failed_objects:
                        print(
                            f"Failed to add object with UUID {failed_object.original_uuid}: {failed_object.message}"
                        )
            else:
                # Use parallel batch insertion with progress tracking for larger datasets
                counter = 0

                # Calculate optimal number of workers based on dataset size
                # Ensure each batch is at least MIN_BATCH_SIZE
                max_possible_workers = max(1, num_objects // MIN_BATCH_SIZE)
                num_workers = min(
                    MAX_WORKERS, max_possible_workers, 16
                )  # Cap at 16 for network connections

                # Calculate batch size to ensure each worker gets a reasonable amount of work
                batch_size = max(MIN_BATCH_SIZE, num_objects // num_workers)

                # Create batches
                batches = [
                    data_objects[i : i + batch_size]
                    for i in range(0, len(data_objects), batch_size)
                ]

                if verbose:
                    print(
                        f"Processing {len(data_objects)} objects in {len(batches)} batches using {num_workers} workers"
                        f" (batch size: {batch_size})"
                    )

                all_failed_objects = []
                completed_jobs = 0

                # Process batches in parallel
                with ThreadPoolExecutor(max_workers=num_workers) as executor:
                    futures = []
                    for i, batch in enumerate(batches):
                        futures.append(
                            executor.submit(
                                self.__ingest_batch,
                                batch,
                                cl_collection,
                                vectorizer,
                                vector_dimensions,
                                named_vectors,
                                uuid,
                                i,
                                verbose,
                            )
                        )

                    # Process results as they complete
                    for future in concurrent.futures.as_completed(futures):
                        batch_count, batch_failed = future.result()
                        counter += batch_count
                        all_failed_objects.extend(batch_failed)

                        completed_jobs += 1
                        if verbose:
                            progress = completed_jobs / len(batches) * 100
                            elapsed = time.time() - start_time
                            rate = counter / elapsed if elapsed > 0 else 0
                            print(
                                f"Overall progress: {progress:.1f}% ({completed_jobs}/{len(batches)} batches, {counter}/{num_objects} objects) at {rate:.1f} objects/second"
                            )

                # Handle any failed objects
                if all_failed_objects:
                    for failed_object in all_failed_objects:
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
        named_vectors: Optional[List[str]] = None,
        wait_for_indexing: bool = CreateDataDefaults.wait_for_indexing,
        verbose: bool = CreateDataDefaults.verbose,
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
                    col,
                    limit,
                    cl_map[consistency_level],
                    randomize,
                    skip_seed,
                    vector_dimensions,
                    uuid,
                    named_vectors,
                    verbose,
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
                    col.with_tenant(tenant),
                    limit,
                    cl_map[consistency_level],
                    randomize,
                    skip_seed,
                    vector_dimensions,
                    uuid,
                    named_vectors,
                    verbose,
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

                # Generate new properties for this batch
                # For small batches, simple generation is fine, for larger we can use parallel
                if batch_count > 100:
                    random_objects = self.__generate_data_object(
                        batch_count, is_update=True, verbose=False
                    )
                else:
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
                ret = self.__delete_data(
                    col, limit, cl_map[consistency_level], uuid, verbose
                )
            else:
                click.echo(f"Processing tenant '{tenant}'")
                ret = self.__delete_data(
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
            )
        elif search_type == "keyword":
            # Keyword logic
            response = collection.with_consistency_level(cl).query.bm25(
                query=query,
                return_metadata=MetadataQuery(score=True, explain_score=True),
                limit=num_objects,
            )
        elif search_type == "hybrid":
            # Hybrid logic
            response = collection.with_consistency_level(cl).query.hybrid(
                query=query,
                return_metadata=MetadataQuery(score=True),
                limit=num_objects,
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
                    for key, tenant in col.tenants.get().items()
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
                )
            if ret == -1:

                raise Exception(
                    f"Failed to query objects in class '{col.name}' for tenant '{tenant}'"
                )
