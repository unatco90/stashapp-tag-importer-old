from stashapi.stashbox import StashBoxInterface
from stashapi.stashapp import StashInterface
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from urllib.parse import urlparse
import os
import time
import json
import traceback
import logging

def init_logging():
    global logger

    # Logging
    # Create a custom logger
    logger = logging.getLogger('logger')

    # Set general logging level.
    logger.setLevel(logging.DEBUG)

    # Create handlers.
    consoleHandler = logging.StreamHandler()
    fileHandler = logging.FileHandler('stashdb_tag_importer.log'.format(datetime.now()), 'a', 'utf-8')

    # Set logging level for handlers.
    # consoleHandler.setLevel(logging.INFO)
    consoleHandler.setLevel(logging.DEBUG)
    fileHandler.setLevel(logging.DEBUG)

    # Create formatter and add it to handlers.
    loggerFormat = logging.Formatter(
        '%(asctime)s %(levelname)s: %(message)s',
        "%y-%m-%d %H:%M:%S"
    )
    consoleHandler.setFormatter(loggerFormat)
    fileHandler.setFormatter(loggerFormat)

    # Add handlers to the logger
    logger.addHandler(consoleHandler)
    logger.addHandler(fileHandler)

def fetch_tags():
    global total_count
    stash_box = StashBoxInterface(
        {
            "endpoint": os.environ["STASHBOX_ENDPOINT"],
            "api_key": os.environ["STASHBOX_API_KEY"],
        }
    )

    tag_query = """
        query Tags($input: TagQueryInput!){
            queryTags(input: $input){
                count
                tags {
                    id
                    name
                    aliases
                    description
                }
            }
        }
    """
    variables = {
        "input": {"page": 1, "per_page": 100, "sort": "NAME", "direction": "DESC"}
    }

    initial_request = stash_box.callGQL(tag_query, variables)["queryTags"]
    total_count = int(initial_request["count"])

    all_tags = initial_request["tags"]
    tag_count = 0

    logger.info(f"Fetching tags from StashDB.")
    while tag_count < total_count:
        logger.info(f"Fetching page {variables['input']['page']}, found {len(all_tags)} of {total_count} tags.")
        next_page = stash_box.callGQL(tag_query, variables)["queryTags"]
        for tag in next_page['tags']:
            tag_count += 1
            logger.debug(f"Tag {tag_count}: {tag['name']}")
        all_tags.extend(next_page["tags"])
        time.sleep(0.75)
        variables["input"]["page"] += 1

    logger.info(f"Fetched {tag_count} tags.")

    return all_tags

# cache tags in a tags.json file so we don't need to repeat the requests every time
def load_tags():
    all_tags = None
    tags_file = Path("tags.json")
    if tags_file.is_file():
        with open(tags_file) as fp:
            all_tags = json.load(fp)
    else:
        all_tags = fetch_tags()
        with open("tags.json", "w") as fp:
            json.dump(all_tags, fp)
    return all_tags

def search_for_tag(stashdb_tag):
    logger.info(f"Searching Local StashDB for \"{stashdb_tag['name']}\".")
    # Find current StashDB tag in local Stash instance.
    local_tag = stash_api.find_tag({"name": stashdb_tag["name"]})
    # logger.debug(f"Local Tag Name: \"{local_tag['name']}\"")
    # logger.debug(f"Local Tag ID: \"{local_tag['id']}\"")
    # logger.debug(f"Local Tag Aliases:\n\"{local_tag['aliases']}\"")
    # logger.debug(f"StashDB Tag Name: \"{stashdb_tag['name']}\"")
    # logger.debug(f"StashDB Tag Aliases:\n\"{stashdb_tag['aliases']}\"")
    return local_tag

def logging_heading():
    """Print a heading to the logger."""
    logger.info(f"")
    logger.info(f"--------------------------------------------------------")

def logging_footer():
    """Print a footing to the logger."""
    logger.info(f"--------------------------------------------------------")
    # logger.info(f"")

def create_new_tags(tags):
    """Create tags if they do not exist."""

    for stashdb_tag in tags:
        # Loop over tags fetched from StashDB.
        try:
            local_tag = search_for_tag(stashdb_tag)

            if not local_tag:
                # Create tag if it does not exist.
                # logger.info(f"Local tag \"{stashdb_tag['name']}\" does not exist, creating.")
                # logger.debug(f"StashDB Tag Name: \"{stashdb_tag['name']}\"")
                # logger.debug(f"StashDB Tag Description: \"{stashdb_tag['description']}\"")
                stash_api.create_tag(
                    {
                        "name": stashdb_tag["name"],
                        "description": stashdb_tag["description"],
                    }
                )
                stats["tag_created"] += 1
                # report_stats()
            elif stashdb_tag['name'] in local_tag['aliases']:
                # logger.debug(f"StashDB Tag \"{stashdb_tag['name']}\" found as alias of \"{local_tag['name']}\".")
                # logger.debug(f"Local Tag Aliases: \"{local_tag['aliases']}\".")
                # TODO - write the rest of the function.
        except:
            logging_heading()
            logger.error(f"\nScript failed on tag \"{stashdb_tag}\".\n")
            logger.error(traceback.format_exc())

def merge_tags(tags):
    """Merge tags where the source tag has an alias that should be associated with the destination tag."""
    for stashdb_tag in tags:
        # Loop over tags fetched from StashDB.
        try:
            local_tag = search_for_tag(stashdb_tag)

            if local_tag:
                # Destination tag ID that source tag will be merged into.
                destination_tag_id = local_tag["id"]
                destination_tag_name = local_tag["name"]
                
                for alias in stashdb_tag["aliases"]:
                    # Loop through each alias associated with the current StashDB tag.
                    logger.debug(f"For \"{alias}\" in \"{stashdb_tag['aliases']}\"")
                    
                    # Find alias in local Stash instance.
                    alias_tag_search = stash_api.find_tag({"name": alias})
                    # logger.debug(f"Search Result: \"{alias_tag_search}\"")

                    if alias == alias_tag_search["name"] and alias != destination_tag_name:
                        # If the StashDB Tag alias matches a Local Tag name, merge it into the StashDB Tag.
                        # Make sure the alias is not the same as the destination tag name in the event of redundant tags.
                        logger.info(f"StashDB Tag alias \"{alias}\" matches a Local Tag name \"{alias_tag_search['name']}\".")

                        # Source tag ID of tag that will be merged.
                        source_tag_id = alias_tag_search['id']

                        logger.debug(f"StashDB Tag Name: \"{stashdb_tag['name']}\"")
                        logger.debug(f"StashDB Aliases: \"{stashdb_tag['aliases']}\"")
                        logger.debug(f"StashDB Matched Alias: \"{alias}\"")
                        logger.debug(f"Local Tag Name: \"{alias_tag_search['name']}\"")
                        logger.debug(f"Local Tag Aliases: \"{alias_tag_search['aliases']}\"")
                        logger.debug(f"Source Tag ID: \"{source_tag_id}\"")
                        logger.debug(f"↓ Merging Into ↓")
                        logger.debug(f"StashDB Tag Name: \"{stashdb_tag['name']}\"")
                        logger.debug(f"Destination Tag ID: \"{destination_tag_id}\"")

                        logger.info(f"Merging alias \"{alias_tag_search['name']}\" into tag \"{local_tag['name']}\"")
                        stash_api.merge_tag(
                            {
                                "source": source_tag_id,
                                "destination": destination_tag_id,
                            }
                        )
                        stats["tag_merged"] += 1
                else:
                    logger.info(f"Aliases are up to date.")
                report_stats()
        except:
            logging_heading()
            logger.error(f"\nScript failed on tag \"{stashdb_tag}\".\n")
            logger.error(traceback.format_exc())

def migrate_alias_update_stashdb(update_type, migration_list, migrate_tag):
    """Update StashDB scenes, galleries, and performers."""
    for item in migration_list:
        # migration_list is the list of scenes, galleries, and performers to be updated.
        tags_to_migrate = []
        for tag in item['tags']:
            # Add each existing tag to a list so we can add it back to the original
            # ID, plus the tag we are migrating.
            tags_to_migrate.append(tag['id'])
        
        if not migrate_tag['id'] in tags_to_migrate:
            # If the tag already exists in the place we're migrating to, skip migration.
            tags_to_migrate.append(migrate_tag['id'])

            update_dict = {
                # Documentation for "tag_ids" here under func scenePartialFromInput:
                # https://github.com/stashapp/stash/blob/develop/internal/api/resolver_mutation_scene.go
                "id": item['id'],
                "tag_ids": tags_to_migrate,
            }

            if update_type == "scene":
                logger.info(f"Migrating tag \"{migrate_tag['name']}\" to scene \"{item['title']}\".")
                stash_api.update_scene(update_dict)
            elif update_type == "gallery":
                logger.info(f"Migrating tag \"{migrate_tag['name']}\" to gallery \"{item['title']}\".")
                stash_api.update_gallery(update_dict)
            elif update_type == "performer":
                logger.info(f"Migrating tag \"{migrate_tag['name']}\" to performer \"{item['name']}\".")
                stash_api.update_performer(update_dict)
            elif update_type == "marker":
                logger.info(f"Migrating tag \"{migrate_tag['name']}\" to marker ID \"{item['id']}\".")
                logger.info(f"Marker ID \"{item}\"")
                # These additional items are required to update a scene marker
                # so we're just redirecting them from the existing marker to
                # the update_dict.
                update_dict["title"] = item['title']
                update_dict["seconds"] = item['seconds']
                update_dict["scene_id"] = item['scene']['id']
                update_dict["primary_tag_id"] = item['primary_tag']['id']
                stash_api.update_scene_marker(update_dict)


def migrate_alias(old_tag, new_tag, alias):
    """ Migrate alias from an old tag, to a new tag.

    This function does the following, in this order.
    # 1. Finds scenes, markers, galleries, and performers with the old tag, and
    ADDS the new tag to them.
    2. Migrates the alias from the old tag to the new tag.

    Step 2 is explicitly performed after step 1 in the event that the 
    process needs to be interrupted, it can be restarted without
    data loss.
    """
    # Get fresh data from the old tag and new tag.
    old_tag = search_for_tag(old_tag)
    new_tag = search_for_tag(new_tag)
    logger.info(f"Mirating alias \"{alias}\" from \"{old_tag['name']}\" to \"{new_tag['name']}\".")
    logger.debug(f"Old Tag: \"{old_tag}\".")
    logger.debug(f"New Tag: \"{new_tag}\".")

    # Store old tag ID in a dict for us to filter search against.
    old_tag_dict = {
        # This dict is a HierarchicalMultiCriterionInput.
        # CriterionModifier and HierarchicalMultiCriterionInput Documentation:
        # https://github.com/stashapp/stash/blob/develop/pkg/models/filter.go
        "value": old_tag["id"],
        "modifier": "INCLUDES", # "modifier" accepts CriterionModifier values.
    }

    # Search filter dict, containing our old tag ID to filter search against.
    search_filter = {
        # This dict is a SceneFilterType.
        # SceneFilterType Documentation:
        # https://github.com/stashapp/stash/blob/develop/pkg/models/scene.go
        # Tags must be in their own dict.
        # Add multiple "tags" entries with their own unique dict to add tags to search.
        "tags": old_tag_dict,
    }

    # Search for scenes with our search_filter > old_tag_dict combo.
    logger.info(f"Mirating tag \"{new_tag['name']}\" to scenes tagged with \"{old_tag['name']}\".")
    scenes_to_migrate = stash_api.find_scenes(search_filter, {"per_page": -1, "sort": "title", "direction": "ASC"})
    migrate_alias_update_stashdb("scene", scenes_to_migrate, new_tag)

    logger.info(f"Mirating tag \"{new_tag['name']}\" to galleries tagged with \"{old_tag['name']}\".")
    galleries_to_migrate = stash_api.find_galleries(search_filter, {"per_page": -1, "sort": "title", "direction": "ASC"})
    migrate_alias_update_stashdb("gallery", galleries_to_migrate, new_tag)

    logger.info(f"Mirating tag \"{new_tag['name']}\" to performers tagged with \"{old_tag['name']}\".")
    performers_to_migrate = stash_api.find_performers(search_filter, {"per_page": -1, "sort": "name", "direction": "ASC"})
    migrate_alias_update_stashdb("performer", performers_to_migrate, new_tag)    

    logger.info(f"Mirating tag \"{new_tag['name']}\" to markers tagged with \"{old_tag['name']}\".")
    markers_to_migrate = stash_api.find_scene_markers(search_filter)
    migrate_alias_update_stashdb("marker", markers_to_migrate, new_tag) 

    # Remove alias from old tag.
    logger.debug(f"Old Tag Aliases: \"{old_tag['aliases']}\".")
    old_tag['aliases'].remove(alias)
    logger.debug(f"Old Tag Aliases: \"{old_tag['aliases']}\".")
    logger.debug(f"Old Tag ID: \"{old_tag['id']}\".")
    stash_api.update_tag(
        {
            "id": old_tag["id"],
            "aliases": old_tag['aliases'],
        }
    )

    # Add alias to new tag.
    logger.debug(f"New Tag Aliases: \"{new_tag['aliases']}\".")
    new_tag['aliases'].append(alias)
    logger.debug(f"New Tag Aliases: \"{new_tag['aliases']}\".")
    logger.debug(f"New Tag ID: \"{new_tag['id']}\".")
    stash_api.update_tag(
        {
            "id": new_tag["id"],
            "aliases": new_tag['aliases'],
        }
    )


def arrange_aliases(tags):
    # Loop over tags scraped from StashDB.
    # logger.debug(f"tags: \"{tags}\"")
    for stashdb_tag in tags:
        # logger.debug(f"stashdb_tag: \"{stashdb_tag}\"")
        try:
            logging_heading()
            local_tag = search_for_tag(stashdb_tag)
            # logger.debug(f"local_tag: \"{local_tag}\"")
            logging_footer()

            if local_tag:
                # Loop through each alias associated with the current StashDB tag.
                for alias in stashdb_tag["aliases"]:
                    # Loop through each alias associated with the current StashDB tag.
                    # logger.debug(f"For \"{alias}\" in \"{stashdb_tag['aliases']}\"")
                    
                    # Find alias in local Stash instance.
                    alias_tag_search = stash_api.find_tag({"name": alias})
                    # logger.debug(f"Search Result: \"{alias_tag_search}\"")

                    if not alias_tag_search:
                        logging_heading()
                        # If the StashDB alias does not exist as a local alias
                        # or tag, we need to add it to a list and create it.
                        logger.info(f"Local alias \"{alias}\" does not exist, creating.")
                        logger.info(f"local_tag['aliases'] \"{local_tag['aliases']}\"")
                        local_tag['aliases'].append(alias)
                        logger.info(f"local_tag['aliases'] \"{local_tag['aliases']}\"")
                        logger.info(f"local_tag['id'] \"{local_tag['id']}\"")
                        stash_api.update_tag(
                            {
                                "id": local_tag["id"],
                                "aliases": local_tag['aliases'],
                            }
                        )
                        stats["alias_created"] += 1
                        logging_footer()
                    elif alias in alias_tag_search["aliases"]:
                        # If the StashDB alias is found as an alias for an existing local tag,
                        # we need to migrate it.
                        if not alias_tag_search['id'] == local_tag['id']:
                            # Make sure we don't attempt to migrate aliases to the same tag.
                            logging_heading()
                            migrate_alias(alias_tag_search, local_tag, alias)
                            logging_footer()
                    # elif alias in alias_tag_search["name"]:

        except:
            logging_heading()
            logger.error(f"\nScript failed on tag \"{stashdb_tag}\".\n")
            logger.error(traceback.format_exc())


def init_stats():
    """Initialize stats dict."""
    global stats
    stats = {
        "tag_created": 0,
        "mismatched_tag_name": 0,
        "alias_created": 0,
        "tag_merged": 0,
        "mismatched_alias_parent": 0,
        "tag_updated": 0,
        "tag_not_updated": 0,
    }

def report_stats():
    """Print out stats."""
    logger.info(f"")
    logger.info(f"------------------------:")
    logger.info(f"Tags Created            : {stats['tag_created']}")
    logger.info(f"Aliases Created         : {stats['alias_created']}")
    logger.info(f"Tags Merged             : {stats['tag_merged']}")
    logger.info(f"Mismatched Tags Fixed   : {stats['mismatched_tag_name']}")
    logger.info(f"Mismatched Aliases Fixed: {stats['mismatched_alias_parent']}")
    logger.info(f"------------------------:")
    logger.info(f"Tags Updated            : {stats['tag_updated']}")
    logger.info(f"Tags Not Updated        : {stats['tag_not_updated']}")
    logger.info(f"------------------------:")
    logger.info(f"")

def main():
    """Main logic loop."""
    global stash_url, stash_api
    load_dotenv()
    stash_url = urlparse(os.environ["STASHAPP_URL"])
    stash_api = StashInterface(
        {
            "scheme": stash_url.scheme,
            "domain": stash_url.hostname,
            "port": stash_url.port,
            "ApiKey": os.environ["STASHAPP_API_KEY"],
        }
    )

    init_logging()
    init_stats()
    tags = load_tags()

    # Work Block
    create_new_tags(tags)
    merge_tags(tags)
    arrange_aliases(tags)
    report_stats()

if __name__ == "__main__":
    main()
