#!/usr/bin/env python
import sys
import ConfigParser
import optparse
import gearman

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from magic_options import MagicOptions

from es_sqla.dbase import adjust_schemas
from crm_worker import CRMWorker

config_defaults = {}


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()

    parser = optparse.OptionParser()
    parser.add_option("-f", "--config", dest="config", default='gsync.ini', help="config file")
    parser.add_option("-s", "--section", dest="section", default='beta', help="config file section")
    parser.add_option("-t", "--test", dest="test", default=None, help="Send test")
    cmd_line_opts, args = parser.parse_args()
    config.readfp(open(cmd_line_opts.config))

    config_defaults = {'odoo_host': 'localhost',
                       'odoo_protocol': 'xmlrpc',
                       'odoo_port': 8069,
                       'odoo_user': 'admin',
                       'odoo_db': 'easy1'}

    mopts = MagicOptions(config_defaults, dict(config.items(cmd_line_opts.section)), vars(cmd_line_opts))

    if mopts.test:
        client = gearman.GearmanClient(mopts.gearman.split(','))
        test_data = config.get('tests', mopts.test)
        print "test data '%s'", test_data
        jr = client.submit_job(mopts.service, test_data, background=True)
        print "submitted some test data to ", mopts.service, ".. Exiting"
        sys.exit(1)

    app_engine = create_engine(mopts.uri)
    adjust_schemas(staging=mopts.staging_schema, model=mopts.model_schema)
    make_session = sessionmaker(bind=app_engine)  # autocommit=True)

    gm_worker = gearman.GearmanWorker(mopts.gearman.split(','))
    crm_task = CRMWorker(mopts, gm_worker, make_session())
    gm_worker.work()
