import os

from dotenv import load_dotenv

from custom_simple_salesforce import Sf

load_dotenv()

# env
domain = os.environ.get("domain")
username = os.environ.get("username")
password = os.environ.get("password")
security_token = os.environ.get("security_token")

client_id = os.environ.get("client_id")
client_secret = os.environ.get("client_secret")


def main():
    try:
        # https://help.salesforce.com/s/articleView?id=xcloud.remoteaccess_oauth_client_credentials_flow.htm&type=5
        credential_yaml = f"""
        auth_method: client_credentials
        client_id: {client_id}
        client_secret: {client_secret}
        domain: {domain}
        """

        id_json = {
            "auth_method": "password",
            "username": username,
            "password": password,
            "security_token": security_token,
            "domain": "login",
        }

        id_json_str = f"""{{
            "auth_method": "password",
            "username": {username},
            "password": {password},
            "security_token": {security_token},
            "domain": "login",
        }}"""

        sf_client = Sf.connection(credential_yaml)
        print("credential_yaml 接続成功！")

        sf_client = Sf.connection(id_json)
        print("username + password dict 接続成功！")

        sf_client = Sf.connection(id_json_str)
        print("username + password dict String 接続成功！")

        _response = sf_client.query("select id, Name from account limit 10")

        for record in _response["records"][:3]:
            print(record)

        # bulk = SfBulk(sf)

        # job = bulk.create_job_query("select id from account")

        # job.poll_status()
        # results_list_dict = job.get_results()

        # for record in results_list_dict[:3]:
        #     print(record)

        # results_list_str = job.get_results(format="reader")

        # for record in results_list_str[:3]:
        #     print(record)

        # results_csv = job.get_results(format="csv")

        # print(results_csv)

        # insert_job = bulk.create_job_insert("Account")
        # print(insert_job.info)

        # csv_data = """Name,Industry
        # Test Account 1,Technology
        # Test Account 2,Finance"""

        # insert_job.upload_data(csv_data)
        # insert_job.close()
        # insert_job.poll_status()

        # if insert_job.is_successful():
        #     if insert_job.has_failed_records():
        #         print("ジョブは完了しましたが、一部のレコードが失敗しました。")
        #         failed_results = insert_job.get_failed_results()
        #         print(failed_results)
        #         # 失敗レコードのログ処理など
        #     else:
        #         print("すべてのレコードが正常に処理されました。")
        #         successful_results = insert_job.get_successful_results()
        #         print(successful_results)
        # elif insert_job.is_failed():
        #     print("ジョブ全体が失敗しました。")
        #     failed_results = insert_job.get_failed_results()
        #     unprocessed_records = insert_job.get_unprocessed_records()
        #     print(insert_job.info)
        #     print(failed_results)
        #     print(unprocessed_records)
        # elif insert_job.is_aborted():
        #     print("ジョブは中断されました。")

        # print("sandbox 接続成功！")
    except Exception as e:
        raise e


if __name__ == "__main__":
    main()
