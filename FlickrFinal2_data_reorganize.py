#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Created on Thu May 04 03:31:51 2017


@author: zhaoy
"""

import os
import os.path as osp
import time
import random
import json
import copy
from subprocess import Popen
from collections import OrderedDict

qshell_path = '../../qshell'
qshell_ak = 'Sj2l3BjGqs47X7fxS_JtrBIsyn2StiV1RI8dppqR'
qshell_sk = 'DXVZR5iqJlHw7EiWTYrsAgmcV4pVrN8Tb0vfO_Lg'
thread_ct = 10 # number of threads for qupload

qrsctl_path = '../../qrsctl'

currentsh_path = os.getcwd()
root_dir = 'FlickrFinal2'
json_dir = 'FlickrFinal2'
created_files_dir = '/workspace/data/__face_datasets__/MegaFace/flickrfinal2_created_files/'
output_jsonlist = 'flickrfinal2_json_list.json'
imgrename_shell = 'imgrename.sh'

qupload_config_file = os.path.join(created_files_dir, 'qupload_config.txt') # config file for upload
qupload_log_file = os.path.join(created_files_dir, 'qupload_log.txt') # log of the upload process

img_ext1 = '.jpg'
img_ext2 = '.jpeg'
json_ext = '.json'

#root_dir = osp.abspath(root_dir)
img_json_template = {'url' : '', 'type' : '', 'label' : {'detect' : {'general_d' : {'bbox' : []}}, 'facecluster' : ''}}

account = 'avatest@qiniu.com'
pw = '25da897892c334ffd6187899a306c14959ae5c0d4552db8da0eb3cbb1e74299a' # password
bkt_name = 'public-dataset'
bkt_url = 'http://datasets.dl.atlab.ai/'
bkt_key_prefix = 'assets/megaface/'
img_json_template = {'url' : '', 'type' : '', 'label' : {'detect' : {'general_d' : {'bbox' : []}}, 'facecluster' : ''}}

cmd_imgrename_list = []
json_log_list = []

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

    upload_imgs(root_dir)

    #os.system(ops.join(created_files_dir,imgrename_shell))

def upload_imgs(file_dir):
    
    output_abs_path = os.path.join(created_files_dir,output_jsonlist)
    imgrename_abs_path = os.path.join(created_files_dir,imgrename_shell)
    path_walk = os.walk(file_dir)
    print('path_walk: {} \n'.format(path_walk))

    for root,dirs,files in path_walk:
        print('for root:{}\n'.format(root))
        #print('for dirs:{}\n'.format(dirs))
        if root!=root_dir and not root.startswith('.'):
           print('--->Processing dir: ' + str(root))
           person_faces_num = 0
           for f in files:
               (filename,fileype) = osp.splitext(f)
               person_identity = str(root).split('/')[-1]
               json_filename = osp.join(root,'{0}{1}'.format(filename,json_ext))
               print('json file name is : ')
               print(json_filename)
               json_file_exist = osp.isfile(json_filename)
               print('json file exist true or false : ')
               print(json_file_exist)
               if (f.endswith(img_ext1) and osp.isfile(json_filename)) or (f.endswith(img_ext2) and osp.isfile(json_filename)):
                  json_url = osp.join(root, 'face_{0}.jpg'.format(person_faces_num))
                  cmd_imgrename = 'mv {0} {1}\n'.format(osp.join(root,f),json_url)
                  print('cmd_imgrename for this img is : ')
                  print(cmd_imgrename)
                  cmd_imgrename_list.append(cmd_imgrename)           
                  #json_url = root + 'face_{0}.jpg'.format(person_faces_num)
                 
                  json_url.replace(currentsh_path,osp.join(bkt_url,bkt_key_prefix)) 
                  with open(json_filename, 'r') as f:
                       fjson = json.load(f)
                  #fjson = json.load(json_filename)
                  
                  converted_bbox = get_bbox_info(fjson['bounding_box'])
                  print('json url is : ')
                  print(json_url)
                  json_log_img = copy.deepcopy(img_json_template)
                  json_log_img['url'] = osp.join(bkt_url,bkt_key_prefix,json_url)
                  json_log_img['type'] = 'image'
                  json_log_img['label']['facecluster'] = person_identity
                  json_log_img['label']['detect']['general_d']['bbox'].append(converted_bbox)
                  print('json_log_for this img is : ')
                  print(json_log_img)
                  json_log_list.append(json_log_img)
                  # write json for img
                  #f_out = open(output_abs_path, 'w')
                  #f_out.write(json.dumps(json_log_img, ensure_ascii = False) + '\n')
                  #f_out.close()
                  person_faces_num = person_faces_num + 1
    print('jsonlist = ')
    print(json_log_list)
    print('imgrenamelist = ')
    print(cmd_imgrename_list)
    #fp = open(output_abs_path, 'w')
    with open(output_abs_path, 'a') as f_out:
        for it in json_log_list:    
            f_out.write(json.dumps(it, ensure_ascii = False) + '\n')
    #for it in json_log_list:
        #fp.write(it)
    #fp.close()
    fp = open(imgrename_abs_path, 'w')
    for it in cmd_imgrename_list:
        fp.write(it)
    fp.close()    
    #os.system('./sh {0}'.format(imgrename_abs_path))
'''
    write_qupload_config_file()
    #upload imgs
    cmd_upload_img = [qshell_path, 'qupload', str(thread_ct), qupload_config_file]
    Popen(cmd_upload_img).wait()
    print 'Successful uploaded batch of images to bucket'
     # batch remove imgs: remove the folder that contains the imgs, because the images have already been uploaded
     #shutil.rmtree(os.path.join(created_files_dir, filename_identifier))

     # upload jsonlist
    output_bkt_key = os.path.join(bkt_key_prefix, output_file_name)
    cmd_upload_jsonlist = [qrsctl_path, 'put', '-c', bkt_name, output_bkt_key, output_abs_path]
    Popen(cmd_upload_jsonlist).wait()
    print 'Uploaded: %s' % output_abs_path
'''
def get_bbox_info(bounding_box):
    x = bounding_box['x']
    y = bounding_box['y']
    width = bounding_box['width']
    height = bounding_box['height']
    face_pts = []
    face_pts.append([x,y]) 
    face_pts.append([x+width,y])
    face_pts.append([x+width,y+height])
    face_pts.append([x,y+height]) 
    converted_bbox = OrderedDict()
    converted_bbox['class'] = 'face'
    converted_bbox['pts'] = face_pts
    return converted_bbox   

# write config file for qupload
def write_qupload_config_file():
    config_json = OrderedDict()
    config_json['src_dir'] = root_dir
    config_json['bucket'] = bkt_name
    config_json['key_prefix'] =bkt_key_prefix
    config_json['rescan_local'] = True
    config_json['skip_suffixes'] = '.tar,.txt'
    config_json['log_file'] = qupload_log_file
    config_json['log_level'] = 'info'
    with open(qupload_config_file, 'w') as f_out:
        f_out.write(json.dumps(config_json, indent = 4) + '\n')
                       
if __name__ == '__main__':
    main()
    print 'Done'
