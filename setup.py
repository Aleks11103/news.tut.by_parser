import os
from setuptools import setup, find_packages

BASE_DIR = os.path.abspath(os.path.dirname(__file__))


def get_version() -> str:
    """Get current version

    Returns:
        str: Current version
    """
    path = os.path.join(BASE_DIR, 'VERSION')
    with open(path, 'r') as version_file:
        return version_file.read().strip()


VERSION = get_version()


def get_license_file() -> str:
    """Get path file with license text

    Returns:
        str: path
    """
    return os.path.join(BASE_DIR, 'LICENSE')


def get_long_description() -> str:
    """Get README.md text

    Returns:
        str: README.md data
    """
    path = os.path.join(BASE_DIR, 'README.md')
    with open(path, 'r') as readme_file:
        return readme_file.read().strip()


def get_requires() -> [str]:
    """Requires packages
    If version is dev - add packages name

    Returns:
        [str]: list with pakages name
    """
    path = os.path.join(BASE_DIR, 'requirements.txt')
    with open(path, 'r') as require_file:
        packages = [
            package.strip()
            for package in require_file.read().strip().split('\n')
        ]

APP_PROPERTY = {
    'name': 'news.tut.by',
    'version': VERSION,
    'author': 'Aliaksei Smantser',
    'author_email': 'lesha_smancer@mail.ru',
    'url': '',
    'packages': find_packages('src', exclude=['tests', '*test*']),
    'package_dir': {'': 'src'},
    'test_suite': 'tests',
    'include_package_data': True,
    'license': get_license_file(),
    'descriprion': 'news.tut.by parser',
    'long_description': get_long_description(),
    'long_description_content_type': 'text/markdown',
    'install_requires': get_requires(),
    'python_requires': '>=3.8',
    'zip_safe': False,
    # 'entry_points': {'console_scripts': ['ad_manager = adm.app.main:run]}
    'classfiers': [
        'Development status :: 5 Production/Stable'
    ]
}


setup(**APP_PROPERTY)