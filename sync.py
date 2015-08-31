import datetime
from es_sqla.model_schema import app
from sqlalchemy import func

from fmapper import field_map


def sync(app_session, oerp):
    # these maxes are not correct. They should check that the row is the 'current' terms based on the dates
    # not the maximum.
    max_term = app_session.query(app.UserTerm.household_id, app.UserTerm.user_id, app.UserTerm.id.label('term_id'),
                                 func.max(app.UserTerm.term_start).label('userterm_term_start')) \
                          .group_by(app.UserTerm.user_id).subquery()

    max_household_rent = app_session.query(app.HouseholdRent.household_id,
                                               app.HouseholdRent.id.label('household_rent_id'),
                                               func.max(app.HouseholdRent.date_term_start).label('householdrent_term_start')) \
                                        .group_by(app.HouseholdRent.household_id).subquery()


    qry = app_session.query(app.User, app.Household, app.TrustAccount, app.Address,
            max_household_rent.c.household_id, max_household_rent.c.household_rent_id, 
                            max_term.c.household_id, max_term.c.term_id, max_term.c.userterm_term_start) \
                     .outerjoin(max_term, max_term.c.user_id == app.User.id) \
                     .outerjoin(app.Household, app.Household.id == max_term.c.household_id) \
                     .outerjoin(max_household_rent, max_household_rent.c.household_id == app.Household.id) \
                     .outerjoin(app.TrustAccount, app.Household.trust_account_id == app.TrustAccount.id) \
                     .outerjoin(app.Address, app.Address.id == app.Household.address_id)


    product_name_to_id = dict([(ii.code, ii.id) for ii in oerp.get('product.product') if ii.code])
    p_obj = oerp.get('res.partner')
    limit = -1
    for res in qry:
        if not limit:
            print "stopping from limit"
            break
        limit -= 1
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
                pass
                #print "nope for ", field, "error '%s" % str(ee)
            except AttributeError as ee:
                pass
                #print "missing attribute for ", field, "error '%s" % str(ee)

        if not es_users:
            oerp.create('res.partner', rec_partner)
        else:
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
                print "es user", res.User.id, "orm user ", user.id
                #if field is 'es_household_rent_id':
                #    from IPython import embed; embed()
                setattr(user, field, value)
                updated += 1

            print "user", user, "id ", user.id, "new", new, "updated", updated, "same", same
#            if updated:
#                from IPython import embed; embed()
            oerp.write_record(user)
