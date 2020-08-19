import json
import os

folders = ['map', 'reduce']
fmap = {
'sourceNum': 0,
'totalVerticesNum': 1,
'totalEdgeNum': 2,
'inputSize': 3,
'numOfExecutors': 4,
'totalMemory': 5,
'totalCores': 6,
'taskParallelism': 7,
'taskVertexCount': 8,
'scheduleGroup': 9,
'isSource': 10,
'sourceCommunicationPattern:Shuffle': 11,
'sourceCommunicationPattern:OneToOne': 12,
'sourceCommunicationPattern:Broadcast': 13,
'sourceDataStore:MemoryStore': 14,
'sourceDataStore:LocalFileStore': 15,
'sourceDataStore:DisaggStore': 16,
'sourceDataStore:Pipe': 17,
'sourceDataFlow:Pull': 18,
'sourceDataFlow:Push': 19,
}


fo = open( 'dataset.txt', 'w' )
for folder in folders:
    for filename in os.listdir(folder):
        with open(os.path.join(folder, filename), 'r') as f:
            try:
                content = json.load(f)
            except:
                print('skipping {}'.format(f))
            data = content['JobMetric']['Plan0']['data']

            irDagSummary = data['irDagSummary'].split('_')
            sourceNum = irDagSummary[0].split('rv')[1]
            totalVerticesNum = irDagSummary[1].split('v')[1]
            totalEdgeNum = irDagSummary[2].split('e')[1]
            inputSize = data['inputSize']

            numOfExecutors = 16
            totalMemory = 224
            totalCores = 64

            for taskID in content['TaskMetric']:
                taskData = content['TaskMetric'][taskID]['data']
                taskDuration = taskData['taskDuration']

                stageID = taskID.split('-')[0]
                
                taskParallelism = data['stage-dag']['vertices'][int(stageID.split('Stage')[1])]['properties']['parallelism']
                taskVertexCount = len(data['stage-dag']['vertices'][int(stageID.split('Stage')[1])]['properties']['irDag']['vertices'])
                scheduleGroup = data['stage-dag']['vertices'][int(stageID.split('Stage')[1])]['properties']['executionProperties']['org.apache.nemo.common.ir.vertex.executionproperty.ScheduleGroupProperty']

                inEdges = [e for e in data['stage-dag']['edges'] if e['dst'] == stageID]
                isSource = 0 if len(inEdges) > 0 else 1
                sourceCommunicationPatternShuffle = 1 if len([e for e in inEdges if 'SHUFFLE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty']]) > 0 else 0
                sourceCommunicationPatternOneToOne = 1 if len([e for e in inEdges if 'ONE_TO_ONE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty']]) > 0 else 0
                sourceCommunicationPatternBroadcast = 1 if len([e for e in inEdges if 'BROADCAST' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.CommunicationPatternProperty']]) > 0 else 0
                sourceDataStoreMemoryStore = 1 if len([e for e in inEdges if 'MEMORY_STORE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty']]) > 0 else 0
                sourceDataStoreLocalFileStore = 1 if len([e for e in inEdges if 'LOCAL_FILE_STORE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty']]) > 0 else 0
                sourceDataStoreDisaggStore = 1 if len([e for e in inEdges if 'GLUSTER_FILE_STORE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty']]) > 0 else 0
                sourceDataStorePipe = 1 if len([e for e in inEdges if 'PIPE' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataStoreProperty']]) > 0 else 0
                sourceDataFlowPull = 1 if len([e for e in inEdges if 'PULL' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty']]) > 0 else 0
                sourceDataFlowPush = 1 if len([e for e in inEdges if 'PUSH' in e['properties']['executionProperties']['org.apache.nemo.common.ir.edge.executionproperty.DataFlowProperty']]) > 0 else 0

                info = {'sourceNum': sourceNum, 'totalVerticesNum': totalVerticesNum, 'totalEdgeNum': totalEdgeNum, 'inputSize': inputSize, 'numOfExecutors': numOfExecutors, 'totalMemory': totalMemory, 'totalCores': totalCores, 'taskParallelism': taskParallelism, 'taskVertexCount': taskVertexCount, 'scheduleGroup': scheduleGroup, 'isSource': isSource, 'sourceCommunicationPattern:Shuffle': sourceCommunicationPatternShuffle, 'sourceCommunicationPattern:OneToOne': sourceCommunicationPatternOneToOne, 'sourceCommunicationPattern:Broadcast': sourceCommunicationPatternBroadcast, 'sourceDataStore:MemoryStore': sourceDataStoreMemoryStore, 'sourceDataStore:LocalFileStore': sourceDataStoreLocalFileStore, 'sourceDataStore:DisaggStore': sourceDataStoreDisaggStore, 'sourceDataStore:Pipe': sourceDataStorePipe, 'sourceDataFlow:Pull': sourceDataFlowPull, 'sourceDataFlow:Push': sourceDataFlowPush}

                fo.write('{}'.format(taskDuration))
                for k,v in info.items():
                    fo.write(' {}:{}'.format(fmap[k], v))
                fo.write('\n')
fo.close()


with open( 'featmap.txt', 'w' ) as fo:
    for k,v in fmap.items():
        fo.write('{} {} i\n'.format(v, k))


