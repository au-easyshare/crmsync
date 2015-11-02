import datetime

from dateutil import tz

from sqlalchemy import or_
from es_sqla.model_schema import app

from fmapper import field_map


def sync(app_session, oerp):
    start_date = datetime.date.today() - datetime.timedelta(days=1)
    qry = app_session.query(app.UserTerm, app.User, app.Household, app.HouseholdRent, app.TrustAccount, app.Address) \
                   .join(app.Household, app.UserTerm.household_id == app.Household.id) \
                   .join(app.User, app.UserTerm.user_id == app.User.id) \
                   .join(app.Address, app.Household.address_id == app.Address.id) \
                   .outerjoin(app.TrustAccount, app.Household.trust_account_id == app.TrustAccount.id) \
                   .outerjoin(app.HouseholdRent, app.Household.id == app.HouseholdRent.household_id) \
                   .filter(or_(app.UserTerm.term_end == None, app.UserTerm.term_end >= start_date)) \
                   .filter(or_(app.HouseholdRent.date_term_end == None, app.HouseholdRent.date_term_end >= start_date))


    product_name_to_id = dict([(ii.code, ii.id) for ii in oerp.get('product.product') if ii.code])
    p_obj = oerp.get('res.partner')
    limit = -1
    for res in qry:
        if not limit:
            print "stopping from limit"
            break
        limit -= 1
        es_user_term = p_obj.search([('es_user_term_id', '=', res.UserTerm.id)])

        rec_partner = dict()
        print "term", res.UserTerm.id
        for field, mapper in field_map.iteritems():
            if mapper is None:  # a field we want to get but not set
                continue
            try:
                if field == 'es_site':
                    if res.User.user_type in product_name_to_id:
                        rec_partner[field] = product_name_to_id[res.User.user_type]
                    else:
                        print "Site not known in product.product", res.User.user_type
                else:
                    rec_partner[field] = mapper(res)
            except ValueError as ee:
                pass
                #print "nope for ", field, "error '%s" % str(ee)
            except AttributeError as ee:
                pass
                #print "missing attribute for ", field, "error '%s" % str(ee)

        if not es_user_term:
            oerp.create('res.partner', rec_partner)
        else:
            user_term = oerp.browse('res.partner', es_user_term[0])
            new = 0
            updated = 0
            same = 0
            for field, value in rec_partner.iteritems():
                if not hasattr(user_term, field):
                    setattr(user_term, field, value)
                    print "new (%s, %s)" % (field, str(value))
                    new += 1
                    continue

                crm_val = getattr(user_term, field)
                if type(crm_val) != type(value):
                    if isinstance(crm_val, datetime.datetime) or isinstance(crm_val, datetime.date):
                        crm_val = str(crm_val)
                    elif isinstance(crm_val, str) and isinstance(value, long):
                        value = str(value)

                # this can be a single if but it's clearer the first is an ORM field
                if hasattr(crm_val, 'id') and crm_val.id == value:
                    same += 1
                    continue
                elif isinstance(crm_val, bool) and crm_val is False and value is None:
                    same += 1
                    continue
                elif value is None and crm_val == 0:
                    same += 1
                    continue
                elif crm_val == value:
                    same += 1
                    continue

                print "Update field '%s' value '%s' crm '%s'" % (field, str(value), crm_val)
                print "es user_term", res.User.id, "orm user_term ", user_term.id
                #if field is 'es_household_rent_id':
                #    from IPython import embed; embed()
                setattr(user_term, field, value)
                updated += 1

            print "user_term", user_term, "id ", user_term.id, "new", new, "updated", updated, "same", same
#            if updated:
#                from IPython import embed; embed()
            oerp.write_record(user_term)
