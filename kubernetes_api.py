from os import path

import yaml
from time import sleep
from kubernetes import client, config

class KubernetesAPI:

    def __init__(self, namespace='ervin'):
        self.namespace = namespace
        config.load_kube_config()

    def create_deployment(self, yaml_file = "postgres-deployment.yaml", app_name='postgres'):
        # Configs can be set in Configuration class directly or using helper
        # utility. If no argument provided, the config will be loaded from
        # default location.
        
        if not self.app_exists(app_name):
            with open(yaml) as f:
                dep = yaml.safe_load(f)
                k8s_apps_v1 = client.ExtensionsV1beta1Api()
                resp = k8s_apps_v1.create_namespaced_deployment(
                    body=dep, namespace=self.namespace)
                print("Deployment created. status='%s'" % resp.metadata.name)
        else: 
            print("Deployment already exists")
    
    def delete_deployment(self, app_name='postgres'):
        try:
            k8s_apps_v1 = client.ExtensionsV1beta1Api()
            resp = k8s_apps_v1.delete_namespaced_deployment('postgres', self.namespace)
            print("Deployment deleted. message='%s'" % resp.message)
        
        except client.rest.ApiException as e:
            print(e)
            print('Deployment failed to delete')


    def app_exists(self, app_name='postgres'):
        if self.get_pods_ip(app_name):
                return True
        for i in range(4):
            sleep(15)
            if self.get_pods_ip(app_name):
                return True
        print(f"No Running pods of App '{app_name}'")
        return False

    def delete_everything(self, app_name):
        api_instance = client.CoreV1Api()
        namespace = self.namespace # str | object name and auth scope, such as for teams and projects
        #pretty = 'pretty_example' # str | If 'true', then the output is pretty printed. (optional)
        #_continue = '_continue_example' # str | The continue option should be set when retrieving more results from the server. Since this value is server defined, kubernetes.clients may only use the continue value from a previous query result with identical query parameters (except for the value of continue) and the server may reject a continue value it does not recognize. If the specified continue value is no longer valid whether due to expiration (generally five to fifteen minutes) or a configuration change on the server, the server will respond with a 410 ResourceExpired error together with a continue token. If the kubernetes.client needs a consistent list, it must restart their list without the continue field. Otherwise, the kubernetes.client may send another list request with the token received with the 410 error, the server will respond with a list starting from the next key, but from the latest snapshot, which is inconsistent from the previous list results - objects that are created, modified, or deleted after the first list request will be included in the response, as long as their keys are after the \"next key\".  This field is not supported when watch is true. Clients may start a watch from the last resourceVersion value returned by the server and not miss any modifications. (optional)
        field_selector = 'status.phase=Running' # str | A selector to restrict the list of returned objects by their fields. Defaults to everything. (optional)
        label_selector = f'app={app_name}' # str | A selector to restrict the list of returned objects by their labels. Defaults to everything. (optional)
        for pod in self.get_pods_ip():
            api_response = api_instance.delete_namespaced_pod(pod['name'], self.namespace)
        
    def delete_pod(self, pod_name):
        api_instance = client.CoreV1Api()
        for i in range(4):
            api_response = api_instance.delete_namespaced_pod(pod_name, self.namespace)
            print(api_response)
            sleep(30)
            for pod in self.get_pods_ip():
                if pod_name == pod['name']:
                    return
        print(f"Pod {pod_name} failed to delete after 4 tries in 2mins")
            
    def get_pods_ip(self, app_name='postgres'):
        api_instance = client.CoreV1Api()
        namespace = self.namespace # str | object name and auth scope, such as for teams and projects
        #pretty = 'pretty_example' # str | If 'true', then the output is pretty printed. (optional)
        #_continue = '_continue_example' # str | The continue option should be set when retrieving more results from the server. Since this value is server defined, kubernetes.clients may only use the continue value from a previous query result with identical query parameters (except for the value of continue) and the server may reject a continue value it does not recognize. If the specified continue value is no longer valid whether due to expiration (generally five to fifteen minutes) or a configuration change on the server, the server will respond with a 410 ResourceExpired error together with a continue token. If the kubernetes.client needs a consistent list, it must restart their list without the continue field. Otherwise, the kubernetes.client may send another list request with the token received with the 410 error, the server will respond with a list starting from the next key, but from the latest snapshot, which is inconsistent from the previous list results - objects that are created, modified, or deleted after the first list request will be included in the response, as long as their keys are after the \"next key\".  This field is not supported when watch is true. Clients may start a watch from the last resourceVersion value returned by the server and not miss any modifications. (optional)
        field_selector = 'status.phase=Running' # str | A selector to restrict the list of returned objects by their fields. Defaults to everything. (optional)
        label_selector = f'app={app_name}' # str | A selector to restrict the list of returned objects by their labels. Defaults to everything. (optional)
        #limit = 56 # int | limit is a maximum number of responses to return for a list call. If more items exist, the server will set the `continue` field on the list metadata to a value that can be used with the same initial query to retrieve the next set of results. Setting a limit may return fewer than the requested amount of items (up to zero items) in the event all requested objects are filtered out and kubernetes.clients should only use the presence of the continue field to determine whether more results are available. Servers may choose not to support the limit argument and will return all of the available results. If limit is specified and the continue field is empty, kubernetes.clients may assume that no more results are available. This field is not supported if watch is true.  The server guarantees that the objects returned when using continue will be identical to issuing a single list call without a limit - that is, no objects created, modified, or deleted after the first request is issued will be included in any subsequent continued requests. This is sometimes referred to as a consistent snapshot, and ensures that a kubernetes.client that is using limit to receive smaller chunks of a very large result can ensure they see all possible objects. If objects are updated during a chunked list the version of the object that was present at the time the first list result was calculated is returned. (optional)
        #resource_version = 'resource_version_example' # str | When specified with a watch call, shows changes that occur after that particular version of a resource. Defaults to changes from the beginning of history. When specified for list: - if unset, then the result is returned from remote storage based on quorum-read flag; - if it's 0, then we simply return what we currently have in cache, no guarantee; - if set to non zero, then the result is at least as fresh as given rv. (optional)
        #timeout_seconds = 56 # int | Timeout for the list/watch call. This limits the duration of the call, regardless of any activity or inactivity. (optional)
        #watch = true # bool | Watch for changes to the described resources and return them as a stream of add, update, and remove notifications. Specify resourceVersion. (optional)

        try: 
            #api_response = api_instance.list_namespaced_pod(namespace, pretty=pretty, _continue=_continue, field_selector=field_selector, label_selector=label_selector, limit=limit, resource_version=resource_version, timeout_seconds=timeout_seconds, watch=watch)
            api_response = api_instance.list_namespaced_pod(namespace,field_selector=field_selector, label_selector=label_selector)
            list_ip = []
            for pod in api_response.items:
                list_ip.append({'ip':pod.status.pod_ip, 'name':pod.metadata.name})

            print(list_ip)
            return list_ip
            #pprint(api_response)
        except ApiException as e:
            print("Exception when calling CoreV1Api->list_namespaced_pod: %s\n" % e)

#pods = KubernetesAPI().get_pods_ip()
#KubernetesAPI().delete_pod(app_name=pods[0]['name'])

# if __name__ == '__main__'
#     main()

# import kubernetes as kb

# kb.config.load_kube_config()

# v1 = kb.client.CoreV1Api()
# print("Listing pods with their IPs:")
# ret = v1.list_pod_for_all_namespaces(watch=False)
# for i in ret.items:
#     print("%s\t%s\t%s" % (i.status.pod_ip, i.metadata.namespace, i.metadata.name))