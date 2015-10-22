
from ..tests_utils import TestCase
from pybabe import Babe


class TestHTML(TestCase):
    s = "a,b\n1,2\n"

    def test_html(self):
        a = Babe().pull(string=self.s, format="csv")
        self.assertEqual(a.to_string(format="html"), """<h2></h2><table>
<tr><th>a</th><th>b</th></tr>
<tr><td>1</td><td>2</td></tr>
</table>
""")
