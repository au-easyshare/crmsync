import functools
import json
import oerplib

# gm_worker = gearman.GearmanWorker(['prod1.internal.easy-share.com.au'])

# otable = {'user': puser}

def handle_lead(jsd, crm):
    p_obj = crm.oerp.get('res.partner')
    email = jsd['email']
    es_users = p_obj.search([('email', '=', email)])
    if not es_users:
        # user_id for flase to make it unassigned
        lead_id = crm.oerp.create('crm.lead', {'user_id': False, 'name': email, 'email': email, 'type': 'lead'})

def handle_user(jsd, oerp):
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
        self.template_user_id = self.oerp.search('res.partner', [('name', '=', 'Template User')])[0]
        self.template_user = self.oerp.read('res.partner', [self.template_user_id])

    def gm_task(self, gearman_worker, gearman_job):
        print 'job', gearman_job
        try:
            jsd = json.loads(gearman_job.data)
            if 'object' not in jsd:
                raise Exception("no 'object'")
            if jsd['object'] not in objects:
                raise Exception("unsupported object '%s'" % jsd['object'])
            objects[jsd['object']](jsd, self)
        except Exception as ee:
            print "bad", str(ee)
            ret = json.dumps(dict(status='error', message=str(ee)))
        else:
            return json.dumps(dict(status='OK'))
        return ret
