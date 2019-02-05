# How the Python SDK documentation works

The generated API documentation is built with [Sphinx](http://www.sphinx-doc.org/en/master/), and is hosted on [Read the Docs](https://readthedocs.org/).

It uses the following:

* Docstrings within the code. Docstrings can use any of the markup supported by Sphinx.
* The `.rst` files in the `docs` directory. These provide the overall page structure.
* The `conf.py` file containing Sphinx settings.

## What to document

Every public class, method, and module should have a docstring. Classes and methods with no docstring will not be included in the API docs.

"Public" here means things that we want third-party developers to use. The SDK also contains many modules and classes that are not actually private (i.e. they aren't prefixed with `_`), but are for internal use only and aren't supported for any other use (we would like to reduce the amount of these in future).

To add an undocumented class or method in an existing module to the docs, just give it a docstring.

To add a new module to the docs, give it a docstring and then add a link to it in the appropriate `api-*.rst` file, in the same format as the existing links.

## Undocumented things

Modules that contain only implementation details are omitted from the docs by simply not including links to them in the `.rst` files.

Internal classes in a documented module will be omitted from the docs if they do not have any docstrings, unless they inherit from another class that has docstrings. In the latter case, the way to omit them from the docs is to edit the `.rst` file that contains the link to that module, and add a `:members:` directive under the module that specifically lists all the classes that  _should_ be shown.

## Testing

In the `docs` directory, run `make html` to build all the docs. Then view `docs/build/html/index.html`.
