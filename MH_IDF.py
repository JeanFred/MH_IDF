# -*- coding: latin-1 -*-

"""Partnership with the MHNT."""

__authors__ = 'User:Jean-Frédéric'

import os
import sys
import simplejson as json
from StringIO import StringIO
from uploadlibrary import metadata
import uploadlibrary.PostProcessing as commonprocessors
from uploadlibrary.UploadBot import DataIngestionBot, UploadBotArgumentParser, make_title
reload(sys)
sys.setdefaultencoding('utf-8')
with open('insee2commonscat.json', 'r') as f:
    insee2commonscat = json.load(f)

front_titlefmt = ""
variable_titlefmt = "%(edif)s - %(leg)s - %(com)s"
rear_titlefmt = " - Médiathèque de l'architecture et du patrimoine - %(ref)s"

class MHIDFMetadataCollection(metadata.MetadataCollection):

    """Handling the metadata collection."""

    def handle_record(self, image_metadata):
        """Handle a record."""
        url = image_metadata['filename']
#        image_metadata['subst'] = 'subst:'
        record = metadata.MetadataRecord(url, image_metadata)
        title = make_title(image_metadata, front_titlefmt,
                           rear_titlefmt, variable_titlefmt)
        record.metadata['commons_title'] = title
        return record

def look_for_MH_titles(separator=" ; "):
    return look_for_MH_titles_i, {'separator': separator}


def look_for_MH_titles_i(field, old_field_value, separator=" ; "):
    results = {}
    for x in old_field_value.split(separator):
        if x.startswith('PA'):
            results['merimee_id'] = x.strip()
        if x.startswith('IA'):
            results['merimee_id_bis'] = x.strip()
        elif x.startswith('PM'):
            results['palissy_id'] = x.strip()
        elif x.startswith('IM'):
            results['palissy_id_bis'] = x.strip()
        else:
            continue
    return results


def insee_to_commonscat():
    return insee_to_commonscat_i, {}


def insee_to_commonscat_i(field, old_field_value):
    new_value = dict()
    new_value[field] = old_field_value
    try:
        commons_cat = insee2commonscat[old_field_value]
        new_value['categories'] = commons_cat
    except:
        pass
    return new_value


def main(args):
    """Main method."""
    collection = MHIDFMetadataCollection()
#    csv_file = 'photographies-serie-monuments-historiques-1851-a-1914.csv'
    csv_file = 'error.csv'    
    collection.retrieve_metadata_from_csv(csv_file, delimiter=';')
    alignment_template = 'User:Jean-Frédéric/AlignmentRow'.encode('utf-8')

    if args.prepare_alignment:
        for key, value in collection.count_metadata_values().items():
            collection.write_dict_as_wiki(value, key, 'wiki',
                                          alignment_template)

    if args.post_process:
        mapping_fields = ['autp', 'datpv', 'edif', 'lieucor']
        mapper = commonprocessors.retrieve_metadata_alignments(mapping_fields,
                                                               alignment_template)
        mapping_methods = {
            'wgs84': commonprocessors.split_and_keep_as_list(separator=','),
            'lbase': look_for_MH_titles(separator=';'),
            'autp': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'datpv': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'edif': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'lieucor': (commonprocessors.process_with_alignment, {'mapper': mapper}),
            'insee': insee_to_commonscat(),
            }
        categories_counter, categories_count_per_file = collection.post_process_collection(mapping_methods)
        print metadata.categorisation_statistics(categories_counter, categories_count_per_file)

        reader = iter(collection.records)
        template_name = 'User:Jean-Frédéric/MH_IDF/Ingestion'.encode('utf-8')
        uploadBot = DataIngestionBot(reader=reader,
                                     front_titlefmt=front_titlefmt,
                                     rear_titlefmt=rear_titlefmt,
                                     variable_titlefmt=variable_titlefmt,
                                     pagefmt=template_name)

    if args.upload:
        uploadBot.doSingle()
    elif args.dry_run:
        string = StringIO()
        collection.write_metadata_to_xml(string)
        print string.getvalue()
        #uploadBot.dry_run()


if __name__ == "__main__":
    parser = UploadBotArgumentParser()
    arguments = parser.parse_args()
    main(arguments)
