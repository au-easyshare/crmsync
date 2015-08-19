import functools
import json
import oerplib

from es_sqla.model_schema import app
import fmapper

import household_handler

# gm_worker = gearman.GearmanWorker(['prod1.internal.easy-share.com.au'])

# otable = {'user': puser}


def handle_lead(jsd, crm):
    email = jsd['email']
    es_lead = crm.lead_obj.search([('name', '=', email)])
    if not es_lead:
        # user_id for False to make it unassigned
        crm.oerp.create('crm.lead', {'user_id': False, 'name': email, 'email_from': email, 'type': 'lead'})


def handle_user(jsd, crm):
    id = jsd['id']
    # first get the user, so we can change the lead to a user
    user_details = crm.session.query(app.User, app.UserTerm, app.Household, app.HouseholdRent, app.TrustAccount, app.Address) \
                      .outerjoin(app.UserTerm, app.UserTerm.user_id == app.User.id) \
                      .outerjoin(app.Household, app.Household.id == app.UserTerm.household_id) \
                      .outerjoin(app.HouseholdRent, app.HouseholdRent.household_id == app.Household.id) \
                      .outerjoin(app.TrustAccount, app.Household.trust_account_id == app.TrustAccount.id) \
                      .outerjoin(app.Address, app.Address.id == app.Household.address_id) \
                      .filter(app.User.id == id).first()
    es_lead = crm.lead_obj.search([('name', '=', user_details.User.email)])
    if es_lead:
        crm.oerp.unlink('crm.lead', es_lead)
    rec_partner = fmapper.es_to_crm(user_details)
    fmapper.cru_es_user(crm, rec_partner, user_details)


def handle_trust(jsd, crm):
    print "got an", jsd
    crm_users = crm.partner_obj.search([('es_user_id', '=', jsd['id']['user'])])
    if not crm_users:
        print "user %s not in the CRM, not handling" % jsd['id']['user']
        print "Add code to add the user in this unlikely event"
        return
    crm_user_record = crm.partner_obj[crm_users[0]]
    crm_claim = dict()
    crm_claim['partner_id'] = crm_user_record.id
    crm_claim['name'] = "Change trust account request"
    crm_claim['user_id'] = 5    # make who?
    crm_claim['categ_id'] = crm.oerp.search('crm.case.categ', [('name', '=', 'Trust Account Change')])[0]
    crm_claim['stage'] = crm.oerp.search('crm.case.stage', [('name', '=', 'New')])[0]
    crm_claim['partner_phone'] = crm_user_record.phone
    crm_claim['email_from'] = crm_user_record.email
    # crm.oerp.create('crm.claim', rec_partner)
    crm.oerp.create('crm.claim', crm_claim)
#    from IPython import embed; embed()

objects = {
    'lead': handle_lead,
    'email': handle_lead,
    'user': handle_user,
    'household': household_handler.handle
    'trust': handle_trust
}


class Lex(Exception):
    def __init__(self, message):
        self.message = message

    def __str__(self):
        return self.message


class CRMWorker:
    def __init__(self, options, gm_worker, session, oerp):
        gm_worker.register_task('gear_beta', functools.partial(CRMWorker.gm_task, self))
        self.session = session
        self.oerp = oerp
        # self.template_user_id = self.oerp.search('res.partner', [('name', '=', 'Template User')])[0]
        # self.template_user = self.oerp.read('res.partner', [self.template_user_id])
        self.lead_obj = self.oerp.get('crm.lead')
        self.partner_obj = self.oerp.get('res.partner')
        self.claim_obj = self.oerp.get('crm.claim')

    def gm_task(self, gearman_worker, gearman_job):
        print 'job', gearman_job
        try:
            jsd = json.loads(gearman_job.data)
            if 'object' not in jsd:
                raise Lex("no 'object'")
            if jsd['object'] not in objects:
                raise Lex("unsupported object '%s'" % jsd['object'])
            objects[jsd['object']](jsd, self)
        except Lex as ee:
            print "bad", str(ee)
            ret = json.dumps(dict(status='error', message=str(ee)))
        except ValueError as ee:
            print "bad '%s' on '%s'" % (str(ee), gearman_job.data)
        except Exception as ee:
            print "some other issue '%s'" % str(ee)
        else:
            return json.dumps(dict(status='OK'))
        return ret
