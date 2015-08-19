import datetime
from es_sqla.model_schema import app

from fmapper import field_map


def sync(app_session, oerp):
    qry = app_session.query(app.User, app.UserTerm, app.Household, app.HouseholdRent, app.TrustAccount, app.Address) \
                     .outerjoin(app.UserTerm, app.UserTerm.user_id == app.User.id) \
                     .outerjoin(app.Household, app.Household.id == app.UserTerm.household_id) \
                     .outerjoin(app.HouseholdRent, app.HouseholdRent.household_id == app.Household.id) \
                     .outerjoin(app.TrustAccount, app.Household.trust_account_id == app.TrustAccount.id) \
                     .outerjoin(app.Address, app.Address.id == app.Household.address_id)

    product_name_to_id = dict([(ii.code, ii.id) for ii in oerp.get('product.product') if ii.code])
    p_obj = oerp.get('res.partner')
    for res in qry:
        es_users = p_obj.search([('es_user_id', '=', res.User.id)])

        rec_partner = dict()
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
                print "nope for ", field, "error '%s" % str(ee)
            except AttributeError as ee:
                print "missing attribute for ", field, "error '%s" % str(ee)

        if not es_users:
            oerp.create('res.partner', rec_partner)
        else:
            print "Update"
            user = oerp.browse('res.partner', es_users[0])
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

                print "Update field '%s' value '%s'" % (field, str(value))
                setattr(user, field, value)
                updated += 1

            print "user ", user.id, "new", new, "updated", updated, "same", same
            print "user", user
            oerp.write_record(user)
