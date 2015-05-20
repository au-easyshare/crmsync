#!/usr/bin/env python
import sys
import functools
import ConfigParser
import optparse
import gearman

from magic_options import MagicOptions

from crm_worker import CRMWorker

config_defaults = {}


if __name__ == '__main__':
    config = ConfigParser.ConfigParser()

    parser = optparse.OptionParser()
    parser.add_option("-f", "--config", dest="config", default='gsync.ini', help="config file")
    parser.add_option("-s", "--section", dest="section", default='beta', help="config file section")
    parser.add_option("-t", "--test", dest="test", action="store_true", default=None, help="Send test")
    options, args = parser.parse_args()
    config.readfp(open(options.config))

    config_defaults = {'odoo_host': 'localhost',
                       'odoo_protocol': 'xmlrpc',
                       'odoo_port': 8069,
                       'odoo_user': 'rob@easy-share.com.au',
                       'odoo_db': 'easy1'}

    mopts = MagicOptions(config_defaults, dict(config.items(options.section)), vars(options))

    if mopts.test:
        client = gearman.GearmanClient(mopts.gearman.split(','))
        jr = client.submit_job(mopts.service, mopts.test_data, background=True)
        print "submitted some test data to ", mopts.service, ".. Exiting"
        sys.exit(1)

    gm_worker = gearman.GearmanWorker(mopts.gearman.split(','))
    crm_task = CRMWorker()
    gm_worker.register_task('gear_beta', functools.partial(CRMWorker.gm_task, crm_task))
    gm_worker.work()
