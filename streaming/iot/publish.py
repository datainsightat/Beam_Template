import os
import time 
from google.cloud import pubsub_v1

def run():

    # Replace  with your project id
    project = 'myproject'

    # Replace  with your pubsub topic
    pubsub_topic = 'projects/myproject/topics/iot_topic'

    # Replace with your service account path
    #path_service_account = ''
	
    #os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_service_account    

    # Replace  with your input file path
    #input_file = '/home/Beam_Template/streaming/iot/xy_data.json'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    #with open(input_file, 'rb') as ifp:
        # skip header
        #header = ifp.readline()

    line = b'{"recordUid":"event-f3e19217","recordTimestamp":1641977166586,"spacesTenantId":"spaces-tenant-549be59d","spacesTenantName":"Simulation","partnerTenantId":"Simulation-Retail","eventType":"DEVICE_LOCATION_UPDATE","deviceLocationUpdate":{"device":{"deviceId":"device-qNCqfbaowTbMviFDFEAt2","userId":"","tags":["EmXXXXeeX"],"mobile":"","email":"","gender":"GENDER_NOT_AVAILABLE","firstName":"","lastName":"","postalCode":"","optIns":["TERMS_AND_CONDITIONS"],"attributes":[],"macAddress":"00:39:a7:8f:4a:00","manufacturer":"","os":"Android","osVersion":"","type":"NOT_AVAILABLE","socialNetworkInfo":[],"deviceModel":""},"location":{"locationId":"location-ad0a997","name":"Location - 4081f653","inferredLocationTypes":["FLOOR"],"parent":{"locationId":"location-59c7afa3","name":"Location - 6861a1f0","inferredLocationTypes":["NETWORK","BUILDING"],"parent":{"locationId":"location-8df0d359","name":"Location - f9756373","inferredLocationTypes":["CAMPUS"],"parent":{"locationId":"location-a29c1d51","name":"Location - 6861a1f0","inferredLocationTypes":["CMX"],"parent":{"locationId":"location-e03e5040","name":"Location - 1b03b03b","inferredLocationTypes":["ROOT"],"sourceLocationId":"","apCount":3760},"sourceLocationId":"","apCount":221},"sourceLocationId":"1496017635452","apCount":221},"sourceLocationId":"733542549725119048","apCount":221},"sourceLocationId":"733542549725119049","apCount":76},"ssid":"#XXXeMXXlWXXi","rawUserId":"","visitId":"visit-5648606633851553440","lastSeen":1641975908000,"deviceClassification":"","mapId":"","xPos":372.79483,"yPos":210.72083,"confidenceFactor":328.0,"latitude":-999.0,"longitude":-999.0,"unc":0.0,"maxDetectedRssi":-89,"ipv4":"192.168.36.244","ipv6":[]}}'

    # loop over each record
    for i in range(100):
        event_data = line   # entire line of input CSV is the message
        print('{0}, Publishing {1} to {2}'.format(i, event_data[0:10], pubsub_topic))
        publisher.publish(pubsub_topic, event_data)
        time.sleep(1)

if __name__ == "__main__":

    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8085"
    os.environ["PUBSUB_PROJECT_ID"] = "myproject"
    os.environ["PUBSUB_PATH"] = "/opt/python-pubsub/samples/snippets/"

    run()