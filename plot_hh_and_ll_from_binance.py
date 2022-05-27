import time
import os
import datetime
import pdfkit
import imgkit
from if_asset_is_close_to_hh_or_ll import find_asset_close_to_hh_and_ll
def plot_hh_and_ll_from_binance ():
    start_time=time.time()

    
    potential_higher_high_assets, potential_lower_low_assets=find_asset_close_to_hh_and_ll()
    print("++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("potential_higher_high_assets\n")
    print ( potential_higher_high_assets)
    print ( "potential_lower_low_assets\n" )
    print ( potential_lower_low_assets )
    #time.sleep(10)
    print ( "++++++++++++++++++++++++++++++++++++++++++++++++++" )

    from plot_levels_with_plotly_binance_symbols import plot_levels_with_plotly
    from pathlib import Path
    #options for pdfkit
    options={'orientation':'Portrait',
             'page-width':'5000',
             'no-print-media-type':None,
             'margin-top': '0.75in' ,
             'margin-right': '0.75in' ,
             'margin-bottom': '0.75in' ,
             'margin-left': '0.75in'
             }
    for trading_pair in potential_higher_high_assets:
        try:
            where_to_plot_html = os.path.join ( os.getcwd () ,
                                           'datasets' ,
                                           'plots' ,
                                           'binance_plots_hh' ,
                                            'binance_plots_hh_html',
                                           f'{trading_pair}.html')

            where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_hh' ,
                                               'binance_plots_hh_pdf',
                                               f'{trading_pair}.pdf' )
            where_to_plot_svg = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_hh' ,
                                               'binance_plots_hh_svg' ,
                                               f'{trading_pair}.svg' )

            where_to_plot_png = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_hh' ,
                                               'binance_plots_hh_png' ,
                                               f'{trading_pair}.png' )
            #create directory for hh parent folder if it does not extist
            path_to_databases = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_hh' )
            Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )
            #create directories for all hh images
            formats=['png','svg','pdf','html']
            for img_format in formats:
                path_to_special_format_images_of_hh = os.path.join ( os.getcwd () ,
                                                   'datasets' ,
                                                   'plots' ,
                                                   'binance_plots_hh',
                                                   f'binance_plots_hh_{img_format}')
                Path ( path_to_special_format_images_of_hh ).mkdir ( parents = True ,
                                                                     exist_ok = True )

            print (f"plotting {trading_pair}", "/", len(potential_higher_high_assets))
            plot_levels_with_plotly(trading_pair,where_to_plot_html,path_to_databases)

            #convert html to pdf
            #pdfkit.from_file(where_to_plot_html , where_to_plot_pdf, options=options)

            #convert html to svg
            imgkit.from_file ( where_to_plot_html , where_to_plot_svg )
            imgkit.from_file ( where_to_plot_html , where_to_plot_png, options={'format':'png'} )
        except:
            print (f'Exception in {trading_pair}')
        finally:
            continue
    for trading_pair in potential_lower_low_assets:
        try:
            where_to_plot_html = os.path.join ( os.getcwd () ,
                                           'datasets' ,
                                           'plots' ,
                                           'binance_plots_ll' ,
                                           'binance_plots_ll_html' ,
                                           f'{trading_pair}.html')

            where_to_plot_pdf = os.path.join ( os.getcwd () ,
                                           'datasets' ,
                                           'plots' ,
                                           'binance_plots_ll' ,
                                           'binance_plots_ll_pdf',
                                           f'{trading_pair}.pdf' )

            where_to_plot_svg = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_ll' ,
                                               'binance_plots_ll_svg',
                                               f'{trading_pair}.svg' )

            where_to_plot_png = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_ll' ,
                                               'binance_plots_ll_png',
                                               f'{trading_pair}.png' )


            # create directory for ll parent folder if it does not extist
            path_to_databases = os.path.join ( os.getcwd () ,
                                               'datasets' ,
                                               'plots' ,
                                               'binance_plots_ll' )
            Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

            # create directories for all ll images
            formats = ['png' , 'svg' , 'pdf','html']
            for img_format in formats:
                path_to_special_format_images_of_ll = os.path.join ( os.getcwd () ,
                                                                     'datasets' ,
                                                                     'plots' ,
                                                                     'binance_plots_ll' ,
                                                                     f'binance_plots_ll_{img_format}' )
                Path ( path_to_special_format_images_of_ll ).mkdir ( parents = True ,
                                                                     exist_ok = True )

            print ( f"plotting {trading_pair}" , "/" , len ( potential_lower_low_assets ) )
            plot_levels_with_plotly(trading_pair,where_to_plot_html,path_to_databases)

            # convert html to pdf
            #pdfkit.from_file ( where_to_plot_html , where_to_plot_pdf, options=options )

            # convert html to svg
            imgkit.from_file ( where_to_plot_html , where_to_plot_svg  )
            imgkit.from_file ( where_to_plot_html , where_to_plot_png ,
                               options = {'format': 'png'} )
        except Exception as e:
            print(f'Exception with {trading_pair}',"\n")
            print (e)
        finally:
            continue
    end_time = time.time ()
    overall_time = end_time - start_time
    print ( 'overall time in minutes=' , overall_time / 60.0 )
    from yield_binance_trading_pair import yield_binance_trading_pair
    from pathlib import Path
    # yield_binance_trading_pair ()
    # for symbol in yield_binance_trading_pair ():
    #     print ( 'part after yield_binance_trading_pair is at work' )
    #     print ( "symbol" , symbol )
    #     where_to_plot_var = os.path.join ( os.getcwd () ,
    #                                        'datasets' ,
    #                                        'plots' ,
    #                                        'binance_plots' ,
    #                                        f'{symbol}.png' )
    #     path_to_databases = os.path.join ( os.getcwd () , 'datasets' , 'plots' , 'binance_plots' )
    #     Path ( path_to_databases ).mkdir ( parents = True , exist_ok = True )

    #     plot_levels_with_plotly ( symbol , where_to_plot,path_to_databases )
    finish_time = datetime.datetime.utcfromtimestamp ( time.time () ).strftime ( '%Y-%m-%dT%H:%M:%SZ' )
    print ( 'finished at' , finish_time )
plot_hh_and_ll_from_binance()