
import os.path
import traceback
from config import *
import os
import shutil
warnings.filterwarnings('ignore')

def data_clean(path_to_check):

    if os.path.exists(path_to_check):
        # 检查路径是文件夹还是文件
        if os.path.isdir(path_to_check):
            try:
                # 使用shutil.rmtree来删除文件夹
                shutil.rmtree(path_to_check)
                print(f"已删除文件夹: {path_to_check}")
            except Exception as e:
                print(f"删除文件夹时出错: {e}")
        else:
            try:
                # 使用os.remove来删除文件
                os.remove(path_to_check)
                print(f"已删除文件: {path_to_check}")
            except Exception as e:
                print(f"删除文件时出错: {e}")
    else:
        print(f"路径不存在: {path_to_check}")

def get_file_in_folder(path, file_type, contains=None, filters=[], drop_type=False):
    file_list = os.listdir(path)
    file_list = [file for file in file_list if file_type in file]
    if contains:
        file_list = [file for file in file_list if contains in file]
    for con in filters:
        file_list = [file for file in file_list if con not in file]
    if drop_type:
        file_list = [file[:file.rfind('.')] for file in file_list]

    return file_list


def exec_jobs(job_files, method='download', param=''):
    wrong_signal = 0
    # ===遍历job下所有脚本
    for job_file in job_files:
        # =加载脚本
        cls = __import__('data_job.%s' % job_file, fromlist=('',))
        # =执行download方法，下载数据
        try:
            if param:  # 指定有参数的方法
                getattr(cls, method)(param)
            else:  # 指定没有参数的方法
                getattr(cls, method)()
        except KeyboardInterrupt:
            print('退出')
            exit()
        except BaseException as e:
            msg = f'{job_file}  {method} 任务执行错误：' + str(e)
            print(msg)
            print(traceback.format_exc())
            # send_wechat_work_msg(msg, error_webhook_url)
            wrong_signal += 1
        # print('-' * 20, '分割线', '-' * 20)

    return wrong_signal


def run():

        if runtime_mode == 'current':
            run_time = datetime.now()
        else:
            run_time = datetime.now().replace(minute=0, second=0, microsecond=0)

        if os.environ.get("QC_DATA_CENTER_CLEAN") == "1":
            data_clean(data_center_path)
        job_files = get_file_in_folder(
            os.path.join(root_path, 'data_job'), file_type='.py', filters=['__init__', '数据任务模版'], drop_type=True
        )
        # 执行下载任务
        wrong_signal = exec_jobs(job_files, method='download', param=run_time)
        if wrong_signal:
            raise SystemExit(int(wrong_signal))

        # 本次循环结束
        print('数据更新完毕')


run()
