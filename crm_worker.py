import functools
import json

# gm_worker = gearman.GearmanWorker(['prod1.internal.easy-share.com.au'])

#otable = {'user': puser}

class CRMWorker:
    def __init__(self, options, gm_worker, session):
        gm_worker.register_task('gear_beta', functools.partial(CRMWorker.gm_task, self))
        self.session = session

    def gm_task(self, gearman_worker, gearman_job):
        print 'job', gearman_job
        try:
            jsd = json.loads(gearman_job.data)
            if 'object' not in jsd:
                raise Exception("no 'object'")
        except Exception as ee:
            print "bad", str(ee)
            ret = json.dumps(dict(status='error', message=str(ee)))
        else:
            return json.dumps(dict(status='OK'))
        return ret
