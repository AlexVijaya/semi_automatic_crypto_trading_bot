import os
import shutil
import gc
import traceback
from pathlib import Path
import numpy as np
from PIL import Image
from os import listdir
from os.path import isfile, join
from PyPDF2 import PdfFileMerger

def merge_multiple_pdf_files_into_one(path_to_folder_with_pdfs_converted_from_jpegs,
                                      path_to_folder_with_a_single_final_pdf):

    # Create and instance of PdfFileMerger() class
    merger = PdfFileMerger ()
    # Create a list with file names


    try:
        shutil.rmtree ( path_to_folder_with_a_single_final_pdf )
        pass
    except Exception as e:
        print ( "error deleting folder \n" , e )
        traceback.print_exc()



    Path ( path_to_folder_with_a_single_final_pdf ).mkdir ( parents = True , exist_ok = True )



    all_pdf_files_in_dir_list = [f for f in listdir ( path_to_folder_with_pdfs_converted_from_jpegs )
                                  if isfile ( join ( path_to_folder_with_pdfs_converted_from_jpegs , f ) )]

    print("all_pdf_files_in_dir_list")
    print ( all_pdf_files_in_dir_list )
    pdf_files=[]
    for pdf_file_for_merging in all_pdf_files_in_dir_list:
        pdf_file = os.path.join(path_to_folder_with_pdfs_converted_from_jpegs,pdf_file_for_merging)
        pdf_files.append(pdf_file)
    # Iterate over the list of file names
    print("pdf_files\n",pdf_files)
    for pdf_file in pdf_files:
        # Append PDF files
        print("pdf_file\n",pdf_file)
        merger.append ( pdf_file )
    # Write out the merged PDF
    merger.write ( os.path.join(path_to_folder_with_a_single_final_pdf,"merged_pdf_with_all_pairs.pdf") )
    merger.close ()

def convert_jpeg_files_into_pdf_files(folder_where_jpeg_files_are_located,
                                      path_to_folder_with_pdfs_converted_from_jpegs):



    all_jpeg_files_in_dir_list = [f for f in listdir ( folder_where_jpeg_files_are_located )
                                  if isfile ( join ( folder_where_jpeg_files_are_located , f ) )]
    # all_jpeg_files_in_dir_list.reverse()
    how_many_plots_to_make_in_single_pdf = 10







    try:
        shutil.rmtree ( path_to_folder_with_pdfs_converted_from_jpegs )
        pass
    except Exception as e:
        print ( "error deleting folder \n" , e )



    Path ( path_to_folder_with_pdfs_converted_from_jpegs ).mkdir ( parents = True , exist_ok = True )




    #
    for number_of_jpeg_file in range ( 0 , len(all_jpeg_files_in_dir_list) ):
        name_of_jpeg_file_without_extension=""
        print("number_of_jpeg_file=",number_of_jpeg_file)
        if "jpeg" in all_jpeg_files_in_dir_list[number_of_jpeg_file]:
            name_of_jpeg_file_without_extension=\
                all_jpeg_files_in_dir_list[number_of_jpeg_file].replace(".jpeg","")
        elif "jpg" in all_jpeg_files_in_dir_list[number_of_jpeg_file]:
            name_of_jpeg_file_without_extension=\
                all_jpeg_files_in_dir_list[number_of_jpeg_file].replace(".jpg","")
        else:
            print("no jpeg and no jpg in file name")


        with Image.open(os.path.join(folder_where_jpeg_files_are_located,
                                          all_jpeg_files_in_dir_list[number_of_jpeg_file])) as image:
            image.load()
            im = image.convert ( 'RGB' )
            print("name_of_jpeg_file_without_extension=",name_of_jpeg_file_without_extension)
            im.save(os.path.join(path_to_folder_with_pdfs_converted_from_jpegs,
                    f"{name_of_jpeg_file_without_extension}.pdf"))
            del im
            gc.collect()

    # for i in range ( 0 , len ( all_jpeg_files_in_dir_list ) , how_many_plots_to_make_in_single_pdf ):
    #     image_list=[]
    #     global first_image_in_a_batch_of_pdfs
    #     first_image_in_a_batch_of_pdfs=np.NAN
    #
    #     for number_of_plot_in_single_pdf in range ( 0 , len ( all_jpeg_files_in_dir_list ) ):
    #         print ( "number_of_plot_in_single_pdf=" , number_of_plot_in_single_pdf )
    #         name_of_jpeg_file_without_extension = \
    #             all_jpeg_files_in_dir_list[number_of_plot_in_single_pdf].replace ( ".jpeg" , "" )
    #
    #         with Image.open ( os.path.join ( folder_where_jpeg_files_are_located ,
    #                                          all_jpeg_files_in_dir_list[number_of_plot_in_single_pdf] ) ) as image:
    #             image.load ()
    #             im = image.convert ( 'RGB' )
    #             if number_of_plot_in_single_pdf==i:
    #
    #                 first_image_in_a_batch_of_pdfs=im
    #                 continue
    #             image_list.append(im)
    #             del im
    #             gc.collect()
    #             # im.save ( os.path.join ( folder_where_jpeg_files_are_located ,
    #             #                          "multiple_pairs_in_one_plot" ,
    #             #                          f"{name_of_jpeg_file_without_extension}.pdf" ) )
    #             first_image_in_a_batch_of_pdfs.save(os.path.join(folder_where_jpeg_files_are_located,
    #                                                              "multiple_pairs_in_one_plot",f"{i}.pdf"),
    #                                                 save_all = True ,
    #                                                 append_images = image_list)
    #     del image_list
    #     del first_image_in_a_batch_of_pdfs
    #     gc.collect ()


                # del im
                # gc.collect ()

        #im.close()
    #     if number_of_plot_in_single_pdf==i:
    #
    #         first_image_in_a_batch_of_pdfs=im
    #         continue
    #     image_list.append(im)
    #
    # print ( "first_image_in_a_batch_of_pdfs\n" , first_image_in_a_batch_of_pdfs )

    # first_image_in_a_batch_of_pdfs.save(os.path.join(folder_where_jpeg_files_are_located,
    #                                                  "multiple_pairs_in_one_plot",f"{i}.pdf"),
    #                                     save_all = True ,
    #                                     append_images = image_list)




    print(all_jpeg_files_in_dir_list)

    # image_1 = Image.open ( r'C:\Users\Ron\Desktop\Test\view_1.png' )
    # image_2 = Image.open ( r'C:\Users\Ron\Desktop\Test\view_2.png' )
    # image_3 = Image.open ( r'C:\Users\Ron\Desktop\Test\view_3.png' )
    # image_4 = Image.open ( r'C:\Users\Ron\Desktop\Test\view_4.png' )
    #
    # im_1 = image_1.convert ( 'RGB' )
    # im_2 = image_2.convert ( 'RGB' )
    # im_3 = image_3.convert ( 'RGB' )
    # im_4 = image_4.convert ( 'RGB' )
    #
    # image_list = [im_2 , im_3 , im_4]
    #
    # im_1.save ( r'C:\Users\Ron\Desktop\Test\my_images.pdf' , save_all = True , append_images = image_list )
def convert_multiple_jpegs_into_a_single_pdf_for_daily_and_lower_timeframes():
    folder_where_jpeg_files_are_located = os.path.join ( os.getcwd () ,
                                                         'datasets' ,
                                                         'plots' ,
                                                         'crypto_exchange_mirror_levels_plots' ,
                                                         'crypto_exchange_plots_jpg' )
    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots' ,
                                                                   'crypto_exchange_plots_pdf' )

    convert_jpeg_files_into_pdf_files ( folder_where_jpeg_files_are_located ,
                                        path_to_folder_with_pdfs_converted_from_jpegs )

    folder_where_jpeg_files_are_located = os.path.join ( os.getcwd () ,
                                                         'datasets' ,
                                                         'plots' ,
                                                         'crypto_exchange_mirror_levels_plots_with_lower_time_frames' ,
                                                         'crypto_exchange_plots_jpg' )
    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots_with_lower_time_frames' ,
                                                                   'crypto_exchange_plots_pdf' )
    convert_jpeg_files_into_pdf_files ( folder_where_jpeg_files_are_located ,
                                        path_to_folder_with_pdfs_converted_from_jpegs )



    folder_where_jpeg_files_are_located = os.path.join ( os.getcwd () ,
                                                         'datasets' ,
                                                         'plots' ,
                                                         'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h' ,
                                                         'crypto_exchange_plots_jpg' )
    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h' ,
                                                                   'crypto_exchange_plots_pdf' )
    convert_jpeg_files_into_pdf_files ( folder_where_jpeg_files_are_located ,
                                        path_to_folder_with_pdfs_converted_from_jpegs )


    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots' ,
                                                                   'crypto_exchange_plots_pdf' )
    path_to_folder_with_a_single_final_pdf = os.path.join ( os.getcwd () ,
                                                            'datasets' ,
                                                            'plots' ,
                                                            'crypto_exchange_mirror_levels_plots' ,
                                                            'single_merged_pdf' )
    merge_multiple_pdf_files_into_one ( path_to_folder_with_pdfs_converted_from_jpegs ,
                                        path_to_folder_with_a_single_final_pdf )
    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots_with_lower_time_frames' ,
                                                                   'crypto_exchange_plots_pdf' )
    path_to_folder_with_a_single_final_pdf = os.path.join ( os.getcwd () ,
                                                            'datasets' ,
                                                            'plots' ,
                                                            'crypto_exchange_mirror_levels_plots_with_lower_time_frames' ,
                                                            'single_merged_pdf' )
    merge_multiple_pdf_files_into_one ( path_to_folder_with_pdfs_converted_from_jpegs ,
                                        path_to_folder_with_a_single_final_pdf )

    path_to_folder_with_pdfs_converted_from_jpegs = os.path.join ( os.getcwd () ,
                                                                   'datasets' ,
                                                                   'plots' ,
                                                                   'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h' ,
                                                                   'crypto_exchange_plots_pdf' )
    path_to_folder_with_a_single_final_pdf = os.path.join ( os.getcwd () ,
                                                            'datasets' ,
                                                            'plots' ,
                                                            'crypto_exchange_mirror_levels_plots_with_lower_time_frames_12h' ,
                                                            'single_merged_pdf' )
    merge_multiple_pdf_files_into_one ( path_to_folder_with_pdfs_converted_from_jpegs ,
                                        path_to_folder_with_a_single_final_pdf )


if __name__=="__main__":
    convert_multiple_jpegs_into_a_single_pdf_for_daily_and_lower_timeframes()