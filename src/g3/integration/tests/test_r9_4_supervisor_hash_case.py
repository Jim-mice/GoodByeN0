from __future__ import annotations
import ast
import importlib.util
import pathlib
import unittest
import sys

HERE=pathlib.Path(__file__).resolve().parent
INTEGRATION=HERE.parent

# Exact boolean branch from the production Supervisor; no fabricated outcomes.
def predicate_from_supervisor():
    tree=ast.parse((INTEGRATION/'n0_integration'/'service_supervisor.py').read_text(encoding='utf-8'))
    matches=[]
    for node in ast.walk(tree):
        if not isinstance(node,ast.If):
            continue
        source=ast.unparse(node.test)
        if ('recovery_validation.get' in source and 'recovery_receipt_sha256' in source
                and 'recovery_identity' in source and 'gate.get' in source):
            matches.append(node.test)
    if len(matches)!=1:
        raise AssertionError('Expected exactly one recovery terminal acceptance branch; got '+str(len(matches)))
    return compile(ast.fix_missing_locations(ast.Expression(body=matches[0])), '<supervisor predicate>', 'eval')

class SupervisorHashCaseTest(unittest.TestCase):
    def setUp(self):
        sys.path.insert(0,str(INTEGRATION))
        from n0_integration.service_supervisor import same_sha256
        self.fn=same_sha256
        self.expression=predicate_from_supervisor()
        self.digest='2247f5f8a4ad3d9b95fb2d894e156d3f5d40212bedcaf90021994c6a34b54d7c'
        self.id='bd88b7f7d44d4be88b8c33b06fcf4c5b'
    def reject(self,*,hash=None,id=None,child_code=0,identity='PASS',status='READY',error=None):
        context={'code':child_code,'recovery_identity':identity,
            'recovery_validation': {'receipt': {'recovery_id':self.id if id is None else id},
                                    'recovery_receipt_sha256':self.digest.upper() if hash is None else hash},
            'gate': {'status':status,'recovery_receipt_id':self.id,
                     'recovery_receipt_sha256':self.digest.lower()},
            'recovery_error': error,
            'same_sha256':self.fn}
        return eval(self.expression,{'__builtins__': {}},context)
    def test_identical_sha_different_hex_case_accepted(self):
        self.assertFalse(self.reject())
    def test_mixed_case_accepted(self):
        self.assertFalse(self.reject(hash=self.digest[:12].upper()+self.digest[12:].lower()))
    def test_wrong_sha_rejected(self):
        self.assertTrue(self.reject(hash='A'*64))
    def test_invalid_sha_rejected(self):
        self.assertTrue(self.reject(hash='not a sha'))
    def test_wrong_recovery_id_rejected(self):
        self.assertTrue(self.reject(id='different'))
    def test_gate_hold_rejected(self):
        self.assertTrue(self.reject(status='HOLD'))
    def test_nonzero_child_exit_rejected(self):
        self.assertTrue(self.reject(child_code=20))
    def test_failed_child_identity_rejected(self):
        self.assertTrue(self.reject(identity='MISMATCH'))
    def test_failed_receipt_validator_rejected(self):
        self.assertTrue(self.reject(error='RECOVERY_RECEIPT_FAULT_IDENTITY_MISMATCH'))

if __name__=='__main__': unittest.main()
