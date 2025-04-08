from setuptools import find_packages, setup


setup(
    name='arxiver', 
    version='0.0.1', 
    description='Open-source library of arxiver', 
    author='Yang Wang', 
    author_email='yangwang4work@gmail.com', 
    package_dir={'': 'src'}, 
    packages=find_packages('src'), 
    python_requires='>=3.10.0', 
    keywords='arxiv'
)