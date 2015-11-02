import datetime
from dateutil import tz

from_zone = tz.tzlocal()
to_zone = tz.tzutc()


def string_or_nope(inval):
    if inval:
        return str(inval)
    else:
        raise ValueError('nope')

# sorn = lambda ss: str(ss) if ss else None

def fmt_datetime(indate):
    if indate is None:
        raise ValueError('nope')
    return indate.replace(tzinfo=from_zone).astimezone(to_zone).strftime('%Y-%m-%d %H:%M:%S')


field_map = {
    'id': None,
    'active': lambda oo: True,
    'city': lambda oo: oo.Address.suburb,
    'customer': lambda oo: True,
    'email': lambda oo: oo.User.email,
    'es_user_id': lambda oo: oo.User.id,
    'es_household_id': lambda oo: oo.Household.id,
    'es_household_rent_id': lambda oo: oo.HouseholdRent.id,
    'es_user_term_id': lambda oo: oo.UserTerm.id,
    'es_site': lambda oo: oo.User.user_type,
    'es_act_date': lambda oo: fmt_datetime(oo.User.account_activation_date),
    'es_hh_setup': lambda oo: True if oo.Household.is_setup == 'Y' else False,
    'es_lease_start': lambda oo: string_or_nope(oo.Household.lease_period_start),
    'es_pm_method': lambda oo: oo.User.payment_method,
    'es_rego_date': lambda oo: fmt_datetime(oo.User.registration_date),
    'es_trust_bsb': lambda oo: oo.TrustAccount.bank_bsb,
    'es_trust_ref': lambda oo: oo.TrustAccount.bank_ref_number,
    'phone': lambda oo: oo.User.contact_number_mob,
    'name': lambda oo: oo.User.first_name + ' ' + oo.User.last_name if oo.User.isperson == 'Y' else oo.User.company_name,
    'zip': lambda oo: string_or_nope(oo.Address.postal_code),
    'street': lambda oo: oo.Address.unit_number + ' ' + oo.Address.street_address,
    'street2': lambda oo: oo.Address.state,
    'type': lambda oo: 'contact',
    'is_company': lambda oo: True if oo.User.isperson == 'N' else False
}


def es_to_crm(es_data):
    rec_partner = dict()
    for field, mapper in field_map.iteritems():
        if mapper is None:  # a field we want to get but not set
            continue
        try:
            rec_partner[field] = mapper(es_data)
        except ValueError as ee:
            print "nope for ", field, "error '%s" % str(ee)
        except AttributeError as ee:
            print "missing attribute for ", field, "error '%s" % str(ee)
    return rec_partner


def cru_es_user(crm, rec_partner, es_user):
    es_users = crm.partner_obj.search([('es_user_id', '=', es_user.User.id)])
    if not es_users:
        print "Create"
        crm.oerp.create('res.partner', rec_partner)
    else:
        print "Update"
        user = crm.oerp.browse('res.partner', es_users[0])
        new = 0
        updated = 0
        same = 0
        for field, value in rec_partner.iteritems():
            if not hasattr(user, field):
                setattr(user, field, value)
                print "new (%s, %s)" % (field, str(value))
                new += 1
                continue

            crm_val = getattr(user, field)
            if isinstance(crm_val, datetime.datetime) or isinstance(crm_val, datetime.date):
                crm_val = str(crm_val)

            if crm_val == value:
                same += 1
                continue

            print "Update field '%s' value '%s' was '%s'" % (field, str(value), str(crm_val))
            setattr(user, field, value)
            updated += 1

        # print "user ", user.id, "new", new, "updated", updated, "same", same
        # print "user", user

        print "updated", crm.oerp.write_record(user)
