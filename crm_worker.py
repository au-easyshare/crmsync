import json

# gm_worker = gearman.GearmanWorker(['prod1.internal.easy-share.com.au'])


class CRMWorker:
    def __init__(self):
        pass
    def gm_task(self, gearman_worker, gearman_job):
        print 'job', gearman_job
        try:
            jsd = json.loads(gearman_job.data)
            print "incoming", str(jsd)
        except Exception as ee:
            print "bad", str(ee)
            ret = json.dumps(dict(status='error', message=str(ee)))
        else:
            return json.dumps(dict(status='OK'))
        return ret
    # gm_worker.set_client_id('your_worker_client_id_name')
    # gm_worker.register_task('gear_beta', task_crm)
    # gm_worker.work()
