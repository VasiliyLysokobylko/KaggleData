import json
import tempfile

import urllib3
import certifi
import os
import time
from selenium import webdriver
import html2text
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
import shutil
from pathlib import Path
import zipfile


class KaggleScraper:

    def __init__(self, kaggle_email: str, kaggle_password: str, username: str, key: str):
        self.files_to_download = ['csv']
        self.max_file_size = None
        self.username = username
        self.key = key
        self.auth_driver = None
        self.user_email = kaggle_email
        self.password = kaggle_password
        self.pool_manager = urllib3.PoolManager(
            num_pools=1,
            maxsize=20,
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where()
        )

    def download_competitions(self, output_folder: str):
        """
            Loads all Kaggle competitions into folders,

            Args:
                output_folder(str): Folder to store scrapping results.
        """
        total_count: int = 0
        page: int = 1
        while True:
            competitions_response = self.kaggle_api_call(resource='/competitions/list', method='GET',
                                                         query_params=[('page', page)])
            page += 1
            bucket = json.loads(competitions_response.data.decode('utf8'))
            total_count += len(bucket)
            for competition in bucket:
                try:
                    self.download_competition_item(competition, output_folder)
                except Exception as e:
                    print('Failed competition downloading: ' + competition['ref'] + '\n' + str(e))
                time.sleep(3)
            if len(bucket) == 0:
                if self.auth_driver is not None:
                    self.auth_driver.close()
                    self.auth_driver = None
                return
            print('Total processed:' + str(total_count))

    def download_competition_item(self, competition, parent_folder: str):
        competition_folder = os.path.join(parent_folder, competition['ref'])
        try:
            if not os.path.exists(competition_folder):
                os.makedirs(competition_folder)
            with open(os.path.join(competition_folder, 'competition.json'), 'w') as f:
                f.write(json.dumps(competition, indent=4))
            self.save_data_description(competition_id=competition['ref'], output_folder=competition_folder)
            self.save_evaluation(competition_id=competition['ref'], output_folder=competition_folder)
        except Exception as e:
            print(json.dumps(competition, indent=4))
            print(str(e))

        competition_data_response = self.kaggle_api_call(resource='/competitions/data/list/' + str(competition['id']),
                                                         method='GET')
        competition_data_list = json.loads(competition_data_response.data.decode('utf8'))
        with open(os.path.join(competition_folder, 'competition_data_files.json'), 'w') as f:
            f.write(json.dumps(competition_data_list, indent=4))
        self.accept_rule(url='https://www.kaggle.com/c/' + competition['ref'] + '/rules')
        time.sleep(10)
        for item in competition_data_list:
            _, ext = os.path.splitext(item['ref'])
            if ext[1:] in self.files_to_download:
                try:
                    file_dir = os.path.join(parent_folder, competition['ref'], 'data')
                    self.download_file(file_json=item, file_dir=file_dir, competition=competition)
                    time.sleep(3)
                except Exception as e:
                    print('Download failed: \n' + json.dumps(item, indent=4))
                    print(str(e))

    def download_file(self, file_json, file_dir: str, competition):
        file_path = os.path.join(file_dir, file_json['ref'])
        if not os.path.exists(file_dir):
            os.makedirs(file_dir)
        file_url = '/competitions/data/download/' + str(competition['ref']) + '/' + file_json['name']
        headers = None
        if self.max_file_size is not None and int(file_json['totalBytes']) > self.max_file_size:
            headers = {'Range': 'bytes=%s-%s' % (0, self.max_file_size)}
        response = self.kaggle_api_call(resource=file_url, method='GET', additional_headers=headers)
        try:
            response_data = response.data.decode('utf8')
            if '"message":"TooManyRequests"' in response_data or 'You must accept this competition' in response_data or '"message":"NotFound' in response_data:
                time.sleep(60 * 10)
                self.download_file(file_json=file_json, file_dir=file_dir, competition=competition)
                print(response.data.decode('utf8'))
                return
        except Exception as e:
            print(file_path + '\n' + json.dumps(headers) + '\n' + str(e))
        with open(os.path.abspath(file_path), 'wb') as f:
            f.write(response.data)

    def kaggle_api_call(self, resource, method: str, query_params=None, body=None, post_params=None,
                        additional_headers=None):
        method = method.upper()
        assert method in ['GET', 'HEAD', 'DELETE', 'POST', 'PUT',
                          'PATCH', 'OPTIONS']
        request_body = json.dumps(body)
        base_uri = "https://www.kaggle.com/api/v1"
        headers = self.get_headers()
        if additional_headers is not None:
            headers.update(additional_headers)
        if method.upper() in ['GET', 'HEAD']:
            del headers['Content-Type']
            del headers['Accept']
            return self.pool_manager.request(method=method, url=base_uri + resource, headers=headers,
                                             fields=query_params)
        if post_params is not None:
            del headers['Content-Type']
            return self.pool_manager.request(method=method, url=base_uri + resource, encode_multipart=True,
                                             headers=headers, fields=post_params)
        else:
            preload = True
            return self.pool_manager.request(method=method, url=base_uri + resource, headers=headers, body=request_body,
                                             fields=query_params, preload_content=preload)

    def get_headers(self):
        auth_key = urllib3.util.make_headers(
            basic_auth=self.username + ':' + self.key
        ).get('authorization')
        headers = {'Accept': 'application/json', 'Content-Type': 'application/json',
                   'User-Agent': 'Swagger-Codegen/1/python', "Authorization": auth_key}
        return headers

    def accept_rule(self, url: str):
        if self.auth_driver is None:
            self.auth_driver = webdriver.Chrome(executable_path=os.path.abspath(os.path.join('chromedriver')))
            self.auth_driver.get('https://www.kaggle.com/')
            self.auth_driver.find_element_by_xpath('//*[@id="sign-in-button"]').click()
            self.driver_wait_for(driver=self.auth_driver,
                                 xpath='/html/body/main/div/div/div/div/div/div/div[3]/div/div[2]/div/div[1]/a[1]',
                                 timeout=3)
            self.auth_driver.find_element_by_xpath(
                '/html/body/main/div/div/div/div/div/div/div[3]/div/div[2]/div/div[1]/a[1]').click()
            self.driver_wait_for(driver=self.auth_driver, xpath='//*[@id="identifierId"]', timeout=3)
            self.auth_driver.find_element_by_xpath('//*[@id="identifierId"]').send_keys(self.user_email)
            self.driver_wait_for(driver=self.auth_driver, xpath='//*[@id="identifierNext"]/span/span', timeout=3)
            self.auth_driver.find_element_by_xpath('//*[@id="identifierNext"]/span/span').click()
            self.auth_driver.switch_to.window(self.auth_driver.window_handles[0])
            time.sleep(3)
            self.driver_wait_for(driver=self.auth_driver, xpath='//*[@id="password"]/div[1]/div/div[1]/input',
                                 timeout=7)
            self.auth_driver.find_element_by_xpath('//*[@id="password"]/div[1]/div/div[1]/input').send_keys(
                self.password)
            self.driver_wait_for(driver=self.auth_driver, xpath='//*[@id="passwordNext"]/span/span', timeout=3)
            self.auth_driver.find_element_by_xpath('//*[@id="passwordNext"]/span/span').click()
            time.sleep(5)

        self.auth_driver.get(url)
        try:
            if len(self.auth_driver.find_elements_by_xpath(
                    '/html/body/main/div/div[2]/div/div/div[1]/div[2]/div[1]/div/div/div/div/div/a')) > 0:
                self.auth_driver.find_element_by_xpath(
                    '/html/body/main/div/div[2]/div/div/div[1]/div[2]/div[1]/div/div/div/div/div/a').click()
        except Exception as e:
            print(str(e))

    def driver_wait_for(self, driver, xpath: str, timeout: int):
        WebDriverWait(driver, timeout).until(expected_conditions.presence_of_element_located(
            (By.XPATH, xpath)))

    def save_data_description(self, competition_id: str, output_folder: str):
        driver = webdriver.Chrome(executable_path=os.path.abspath(os.path.join('chromedriver')))
        driver.get('https://www.kaggle.com/c/' + competition_id + '/data');
        element = driver.find_element_by_xpath(
            '/html/body/main/div/div[2]/div/div/div[1]/div[2]/div[1]/div[2]/div[1]')
        html = element.get_attribute('innerHTML')
        h2t = html2text.HTML2Text()
        h2t.body_width = 0
        h2t.protect_links = True
        h2t.wrap_links = False
        md_text = h2t.handle(html)

        with open(os.path.join(output_folder, 'data_description.txt'), 'w') as f:
            f.write(element.text)
        with open(os.path.join(output_folder, 'data_description.md'), 'w') as f:
            f.write(md_text)
        driver.close()

    def save_evaluation(self, competition_id: str, output_folder: str):
        driver = webdriver.Chrome(executable_path=os.path.abspath(os.path.join('chromedriver')))
        driver.get('https://www.kaggle.com/c/' + competition_id + '/overview/evaluation');
        element = driver.find_element_by_xpath(
            '//*[@id="competition-overview__nav-content-container"]/div[2]/div/div')
        html = element.get_attribute('innerHTML')
        h2t = html2text.HTML2Text()
        h2t.body_width = 0
        h2t.protect_links = True
        h2t.wrap_links = False
        md_text = h2t.handle(html)

        with open(os.path.join(output_folder, 'evaluation.txt'), 'w') as f:
            f.write(element.text)
        with open(os.path.join(output_folder, 'evaluation.md'), 'w') as f:
            f.write(md_text)
        driver.close()


class Tools:
    def cut_csv(self, csv_path: str, max_file_size: int):
        _, ext = os.path.splitext(csv_path)
        if os.path.isdir(csv_path):
            for file in os.listdir(csv_path):
                path = os.path.join(csv_path, file)
                self.cut_csv(path, max_file_size)
        elif ext in ['.csv'] and max_file_size < os.path.getsize(csv_path):
            try:
                content = open(csv_path, 'r').read(max_file_size)
                cut_content = content[:content.rindex('\n') + len('\n')]
                with open(csv_path, 'w') as f:
                    f.write(cut_content)
            except Exception as e:
                print(csv_path + '\n' + str(e))

    def unpack_hidden_archives(self, path: str):
        if os.path.isdir(path):
            for file in os.listdir(path):
                self.unpack_hidden_archives(os.path.join(path, file))
        is_skip = False
        report_msg = None
        try:
            open(path, 'r').read().encode('utf8')
            is_skip = True
        except Exception as e:
            report_msg = 'Trying to unpack: ' + path + '\n' + str(e)

        filename, ext = os.path.splitext(path)
        if is_skip or ext not in ['.csv']:
            return
        print(report_msg)
        temp_dir = tempfile.mkdtemp()
        archive_name = None
        try:
            archive_name = filename + '.zip'
            shutil.copyfile(path, archive_name)
            zf = zipfile.ZipFile(archive_name)
            zf.extractall(path=temp_dir)
            parent_folder = Path(path).parent
            for file in os.listdir(temp_dir):
                shutil.copyfile(
                    os.path.join(temp_dir, file),
                    os.path.join(parent_folder, file))
        except Exception as e:
            print('UNPACK FAILED: ' + path + '\n' + str(e))
        finally:
            shutil.rmtree(temp_dir)
            if archive_name is not None:
                os.remove(archive_name)
