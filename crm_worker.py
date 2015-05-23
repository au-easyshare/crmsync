import functools
import json
import oerplib

# gm_worker = gearman.GearmanWorker(['prod1.internal.easy-share.com.au'])

#otable = {'user': puser}

def handle_lead(jsd, erp_con):
    pass

def handle_user(jsd, erp_con):
    pass

objects = {
        'lead': handle_lead,
        'user': handle_user
        }

class CRMWorker:
    def __init__(self, options, gm_worker, session):
        gm_worker.register_task('gear_beta', functools.partial(CRMWorker.gm_task, self))
        self.session = session
        self.oerp = oerplib.OERP(options.odoo_host, protocol=options.odoo_protocol, port=options.odoo_port)
        self.erp_con = self.oerp.login(options.odoo_user, options.odoo_password, options.odoo_db)

    def gm_task(self, gearman_worker, gearman_job):
        print 'job', gearman_job
        try:
            jsd = json.loads(gearman_job.data)
            if 'object' not in jsd:
                raise Exception("no 'object'")
            if jsd['object'] not in objects:
                raise Exception("unsupported object '%s'" % jsd['object'])
            objects[jsd['object']](jsd, self.erp_con)
        except Exception as ee:
            print "bad", str(ee)
            ret = json.dumps(dict(status='error', message=str(ee)))
        else:
            return json.dumps(dict(status='OK'))
        return ret
