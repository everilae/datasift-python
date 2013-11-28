#!/usr/bin/env python

if __name__ == '__main__':
    import unittest

    # Import the tests
    from datasift.tests.test_user import TestUser
    from datasift.tests.test_definition import TestDefinition
    from datasift.tests.test_push import TestPush

    # Run the tests
    unittest.main()
