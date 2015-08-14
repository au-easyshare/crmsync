from es_sqla.model_schema import app

def handle(jsd, crm):
    user_id = jsd['id']['user']
    household_id = jsd['id']['household']

    # first get the user, so we can change the lead to a user
    user_details = crm.session.query(app.User, app.UserTerm, app.Household, app.HouseholdRent, app.TrustAccount) \
                      .outerjoin(app.UserTerm, app.UserTerm.user_id == app.User.id) \
                      .outerjoin(app.Household, app.Household.id == app.UserTerm.household_id) \
                      .outerjoin(app.HouseholdRent, app.HouseholdRent.household_id == app.Household.id) \
                      .outerjoin(app.TrustAccount, app.Household.trust_account_id == app.TrustAccount.id) \
                      .filter(app.User.id == user_id, app.Household.id == household_id).first()

