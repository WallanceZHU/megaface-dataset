#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
created on Thu May 04 03:31:51 2017


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

root_dir = 'identities_all'
json_dir = 'MegafaceIdentities_VGG_META'
created_files_dir = '/workspace/data/__face_datasets__/MegaFace/identities_created_files/'
output_jsonlist = 'identity_json_list_new.json'
imgrename_shell = 'imgrename.sh'

qupload_config_file = os.path.join(created_files_dir, 'qupload_config.txt') # config file for upload
qupload_log_file = os.path.join(created_files_dir, 'qupload_log.txt') # log of the upload process

img_ext = ['.jpg','.jpeg']
json_ext = '.json'

account = 'avatest@qiniu.com'
pw = '25da897892c334ffd6187899a306c14959ae5c0d4552db8da0eb3cbb1e74299a' # password
bkt_name = 'identities-dataset'
bkt_url = 'http://ov4tcjg0k.bkt.clouddn.com/'
bkt_key_prefix = 'assets/megaface/identities_all/'
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


def upload_imgs(file_dir):

	output_abs_path = os.path.join(created_files_dir, output_jsonlist)
	imgrename_abs_path = os.path.join(created_files_dir, imgrename_shell)
        '''
	f_json_out = open(output_abs_path, 'w')
	imgrename_sh = open(imgrename_abs_path, 'w')
        imgrename_abs_path = os.path.join(created_files_dir, imgrename_shell)
	path_walk = os.walk(file_dir)
	print('path_walk: {} \n'.format(path_walk))
	
	for root, dirs, files in path_walk:
		if root == root_dir or root.startswith('.'):
			continue
		print('--->Processing root: ' + str(root))
		person_faces_num = 0
		for f in files:
			(filename, fileext) = osp.splitext(f)
			if fileext not in img_ext:
				continue
			person_identity = str(root).split('/')[-1]
			json_filename = osp.join(json_dir, person_identity, filename+json_ext)
			json_url = osp.join(root, 'face_{0}.jpg'.format(person_faces_num))
			try:
				with open(json_filename, 'r') as fl:
					fjson = json.load(fl)
			except:
				print("json file not exists:",json_filename)
				continue

			converted_bbox = get_bbox_info(fjson['box'], fjson['exp_bb'])
			json_log_img = generate_json(converted_bbox,json_url,person_identity)
			f_json_out.write(json.dumps(json_log_img, ensure_ascii=False) + '\n')
                        #print(f)
                        #tmp = osp.join(root,f)
                        #print(osp.join(root,f))


                        #print(json_url)
                        imgrename = 'mv {0} {1}\n'.format(osp.join(root,f),json_url)
                        imgrename_sh.write(imgrename)
                        person_faces_num = person_faces_num + 1

        f_json_out.close()
        imgrename_sh.close()
		
        #os.system('./sh {0}'.format(imgrename_abs_path))     
	write_qupload_config_file()
	# upload imgs
	cmd_upload_img = [qshell_path, 'qupload', str(thread_ct), qupload_config_file]
	Popen(cmd_upload_img).wait()
	print 'Successful uploaded batch of images to bucket'
	# batch remove imgs: remove the folder that contains the imgs, because the images have already been uploaded
	# shutil.rmtree(os.path.join(created_files_dir, filename_identifier)) #这个要删的话可以通过qshell的参数delete_on_success删除
        '''
	# upload jsonlist
	output_bkt_key = os.path.join(bkt_key_prefix, output_jsonlist)
	cmd_upload_jsonlist = [qrsctl_path, 'put', '-c', bkt_name, output_bkt_key, output_abs_path]
	Popen(cmd_upload_jsonlist).wait()
	print 'Uploaded: %s' % output_abs_path
        
def get_bbox_info(cropped_box,exp_box):
	left = cropped_box['left'] - exp_box['left']
	right = cropped_box['right'] - exp_box['left']
	top = cropped_box['top'] - exp_box['top']
	bottom = cropped_box['bottom'] - exp_box['top']
	face_pts = []
	face_pts.append([left,top])
	face_pts.append([right,top])
	face_pts.append([right,bottom])
	face_pts.append([left,bottom])
	converted_bbox = OrderedDict()
	converted_bbox['class'] = 'face'
	converted_bbox['pts'] = face_pts
        print('face_Pts = ')
        print(face_pts)
	return converted_bbox

def generate_json(box,json_url,person_identity):
	json_log_img = copy.deepcopy(img_json_template)
	json_log_img['url'] = osp.join(bkt_url, bkt_key_prefix, json_url)
	json_log_img['type'] = 'image'
	json_log_img['label']['facecluster'] = person_identity
	json_log_img['label']['detect']['general_d']['bbox'].append(box)
	return json_log_img

# write config file for qupload
def write_qupload_config_file():
	config_json = OrderedDict()
	config_json['src_dir'] = '/workspace/data/__face_datasets__/MegaFace/identities_all'
	config_json['bucket'] = bkt_name
	config_json['key_prefix'] =bkt_key_prefix
	config_json['rescan_local'] = True
	#config_json['skip_path_prefixes'] = 'F,M,d,f,n,s,t,identities_created_files,identities_data_organize.out,identities_data_organize.py,identities_data_organize1.py,identities_data_organize_byx.py,identities_meta.tar.gz,identities_tight_cropped.tar.gz,imgrename.out'
        config_json['skip_suffixes'] = '.tar.gz,.txt'
	config_json['log_file'] = qupload_log_file
	config_json['log_level'] = 'info'
	with open(qupload_config_file, 'w') as f_out:
		f_out.write(json.dumps(config_json, indent=4) + '\n')


if __name__ == '__main__':
	main()
	print 'Done'


