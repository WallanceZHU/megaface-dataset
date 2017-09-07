'''
upload images of the MSCeleb dataset to the bucket according to text files stored on K8S
write infomation about the uploaded images to a jsonlist file that follows the Atflow standard
the uploaded files will be used for training deep learning models
'''

import os
import re
import copy
import json
import shutil
import base64
import struct
from subprocess import Popen
from collections import OrderedDict

qrsctl_path = './qrsctl'
qshell_path = './qshell'
qshell_ak = 'Sj2l3BjGqs47X7fxS_JtrBIsyn2StiV1RI8dppqR'
qshell_sk = 'DXVZR5iqJlHw7EiWTYrsAgmcV4pVrN8Tb0vfO_Lg'
thread_ct = 10 # number of threads for qupload
upload_batch_size = 10000 # upload when this many images have been saved locally 

src_dir = '/workspace/data/' # directory containing the text files that are to be read from
created_files_dir = '/workspace/megaface_created_files/' # directory to store all new files created by this program
MID_name_relation_dir = os.path.join(created_files_dir, 'MID_name_relations/') # stores relation between MID and person name
MID_name_relation_file = os.path.join(MID_name_relation_dir, 'relations.txt') # created by function write_MID_name_relation()

qupload_config_file = os.path.join(created_files_dir, 'qupload_config.txt') # config file for qupload
qupload_log_file = os.path.join(created_files_dir, 'qupload_log.txt') # log of the upload process

input_file_pattern = r'MsCelebV1-(.*).tsv'
output_pattern = 'jsonlist-%s.json' # format of the output jsonlist
face_id_pattern = r'FaceId-(.*)' # pattern of one column in the input text files
face_id_pattern_devset = r'Face_Id(.*)' # pattern of one column in the input text files that end with "devset"

img_json_template = {'url' : '', 'type' : '', 'label' : {'detect' : {'general_d' : {'bbox' : []}}, 'facecluster' : ''}}

account = 'avatest@qiniu.com'
pw = '25da897892c334ffd6187899a306c14959ae5c0d4552db8da0eb3cbb1e74299a' # password
bkt_name = 'public-dataset'
bkt_url = 'http://datasets.dl.atlab.ai/'
bkt_key_prefix = 'assets/msceleb/'

def main():
    # log into qrsctl
    print 'Logging into qrsctl.'
    cmd_login_qrsctl = [qrsctl_path, 'login', account, pw]
    Popen(cmd_login_qrsctl).wait()
    print 'Successfully logged into qrsctl.'

    # log into qshell
    print 'Logging into qshell.'
    cmd_login_qshell = [qshell_path, 'account', qshell_ak, qshell_sk]
    Popen(cmd_login_qshell).wait()
    print 'Successfully logged into qshell.'

    if not os.path.exists(created_files_dir):
        os.makedirs(created_files_dir)

    # write_MID_name_relation('MsCelebV1-ImageThumbnails.tsv')
    upload_imgs('MsCelebV1-ImageThumbnails.tsv', 'thumbnails')
    # upload_imgs('MsCelebV1-Faces-Cropped.tsv', 'cropped')
    # upload_imgs('MsCelebV1-Faces-Cropped-DevSet1.tsv', 'cropped-devset')
    # upload_imgs('MsCelebV1-Faces-Cropped-DevSet2.tsv', 'cropped-devset')
    # upload_imgs('MsCelebV1-Faces-Aligned.tsv', 'aligned')
    # upload_imgs('MsCelebV1-Faces-Aligned-DevSet1.tsv', 'aligned-devset')
    # upload_imgs('MsCelebV1-Faces-Aligned-DevSet2.tsv', 'aligned-devset')

# store relation between MID and the person's name.
# Useful because when we write the jsonlist for images in 'cropped', we need to know the name of the person, but we are 
# only given the MID of the image
def write_MID_name_relation(filename):
    if not os.path.exists(MID_name_relation_dir):
        os.makedirs(MID_name_relation_dir)
    prev_MID = '' # there are a lot of duplicates in the input file
    for line in open(os.path.join(src_dir, filename), 'r'):
        MID, person_name, search_rank, img_url, _, face_data_str = line.split('\t')
        if MID == prev_MID:
            continue
        else:
            with open(MID_name_relation_file, 'a') as f_out:
                f_out.write('%s\t%s\n' % (MID, person_name))
            prev_MID = MID

# read in the text file written by write_MID_name_relation(), 
# and store the relation between MID and name in a dict. return the dict.
def read_MID_name_relation():
    if not os.path.isfile(MID_name_relation_file):
        return
    relation = {}
    for line in open(MID_name_relation_file, 'r'):
        line = line.strip() # remove trailing newline
        MID, name = line.split('\t')
        relation[MID] = name
    return relation

# given an encoded string that contains information on the coordinates of the upper-left and lower-right points of the bounding box,
# decode it with base64 and prepare a dict that represents the bounding box, under the Atflow standard
def get_bbox_info(encoded_face_rectangle):
    # bounding box
    bndbox=struct.unpack("ffff",base64.b64decode(encoded_face_rectangle))
    xlefttop =  bndbox[0]
    ylefttop =  bndbox[1]
    height =  bndbox[2]
    width =  bndbox[3]
    face_pts = []
    face_pts.append([xlefttop, ylefttop])
    face_pts.append([xlefttop+width, ylefttop])
    face_pts.append([xlefttop+width, ylefttop+height])
    face_pts.append([xlefttop, ylefttop+height])
    converted_bbox = OrderedDict()
    converted_bbox['class'] = 'face'
    converted_bbox['pts'] = face_pts
    return converted_bbox

# upload images to bucket based on information in the input file
# write a jsonlist containing information about the uploaded images, using the atflow standard
def upload_imgs(filename, filetype):

   
    
    write_qupload_config_file()

    # 'MsCelebV1-ImageThumbnails.tsv' -> 'ImageThumbnails'
    filename_identifier = re.match(input_file_pattern, filename).group(1)

    output_file_name = output_pattern % filename_identifier
    output_abs_path = os.path.join(created_files_dir, output_file_name)

    if filetype in ['aligned', 'aligned-devset', 'cropped', 'cropped-devset']:
        rel_MID_name = read_MID_name_relation()

    ct_imgs = 0

    for line in open(os.path.join(src_dir, filename), 'r'):
        if filetype == 'thumbnails':
            MID, person_name, search_rank, img_url, _, face_data_str = line.split('\t')
            img_dir = '%s/%s/%s/' % (filename_identifier, MID, person_name.replace(' ', ''))
            img_name = '%s.jpg' % search_rank
            img_path = os.path.join(img_dir, img_name)
            # no bbox information in thumbnails
        elif filetype in ['aligned', 'cropped']:
            MID, search_rank, img_url, _, face_id, face_rectangle, face_data_str = line.split('\t')
            person_name = rel_MID_name[MID]
            face_id_num = re.match(face_id_pattern, face_id).group(1) # a number
            img_dir = '%s/%s/%s/%s/' % (filename_identifier, MID, person_name.replace(' ',''), search_rank)
            img_name = 'face_%s.jpg' % face_id_num
            img_path = os.path.join(img_dir, img_name)
            converted_bbox = get_bbox_info(face_rectangle)
        elif filetype in ['aligned-devset','cropped-devset']:
            MID, person_name, img_url, face_id, face_rectangle, face_data_str = line.split('\t')
            if not MID in rel_MID_name:
                continue
            person_name = rel_MID_name[MID] # do not use person_name in column 2 of the line
            face_id_num = re.match(face_id_pattern_devset, face_id).group(1) # a number
            img_dir = '%s/%s/%s/' % (filename_identifier, MID, person_name.replace(' ',''))
            img_name = 'face_%s.jpg' % face_id_num
            img_path = os.path.join(img_dir, img_name)
            converted_bbox = get_bbox_info(face_rectangle)

        print 'Processing: %s' % img_path
        img_rel_path = os.path.join(created_files_dir, img_path)
        if not os.path.exists(os.path.join(created_files_dir, img_dir)):
            os.makedirs(os.path.join(created_files_dir, img_dir))

        # get image by decoding the string encoded in base64
        image = open(img_rel_path, "wb")
        image.write(face_data_str.decode('base64'))
        image.close()

        ct_imgs += 1

        img_bkt_key = os.path.join(bkt_key_prefix, img_path)
        json_log_img = copy.deepcopy(img_json_template)
        json_log_img['url'] = bkt_url + img_bkt_key
        json_log_img['type'] = 'image'
        json_log_img['label']['facecluster'] = person_name
        if filetype in ['aligned', 'aligned-devset', 'cropped', 'cropped-devset']:
            json_log_img['label']['detect']['general_d']['bbox'].append(converted_bbox)

        # write json for img
        with open(output_abs_path, 'a') as f_out:
            f_out.write(json.dumps(json_log_img, ensure_ascii = False) + '\n')

        # upload imgs
        if ct_imgs == upload_batch_size:
            cmd_upload_img = [qshell_path, 'qupload', str(thread_ct), qupload_config_file]
            Popen(cmd_upload_img).wait()
            ct_imgs = 0
            print 'Successful uploaded batch of images to bucket'
            # batch remove imgs: remove the folder that contains the imgs, because the images have already been uploaded
            shutil.rmtree(os.path.join(created_files_dir, filename_identifier))
    
    # upload remaining imgs
    if ct_imgs > 0:
        cmd_upload_img = [qshell_path, 'qupload', str(thread_ct), qupload_config_file]
        Popen(cmd_upload_img).wait()
        print 'Successful uploaded batch of images to bucket'
        # batch remove imgs: remove the folder that contains the imgs, because the images have already been uploaded
        shutil.rmtree(os.path.join(created_files_dir, filename_identifier))

    # upload jsonlist
    output_bkt_key = os.path.join(bkt_key_prefix, output_file_name)
    cmd_upload_jsonlist = [qrsctl_path, 'put', '-c', bkt_name, output_bkt_key, output_abs_path]
    Popen(cmd_upload_jsonlist).wait()
    print 'Uploaded: %s' % output_abs_path

# write config file for qupload
def write_qupload_config_file():
    config_json = OrderedDict()
    config_json['src_dir'] = created_files_dir
    config_json['bucket'] = bkt_name
    config_json['key_prefix'] =bkt_key_prefix
    config_json['rescan_local'] = True
    config_json['skip_suffixes'] = '.json,.txt'
    config_json['log_file'] = qupload_log_file
    config_json['log_level'] = 'info'
    with open(qupload_config_file, 'w') as f_out:
        f_out.write(json.dumps(config_json, indent = 4) + '\n')

if __name__ == '__main__':
    main()
    print 'Done'


