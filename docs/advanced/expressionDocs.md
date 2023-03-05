As Quality is based on sql it can be useful to document it in place, particularly with Lambda and Output expressions, but also applies to rules and trigger rules.

The basic format follows javadocs / scaladocs approach, without *'s on each line, but is possible to define on one line:

```scala
/** My Description @param name name desc @param othername othername desc @return return val*/ 
```

This could also be written with newlines including markdown (if the renderer supports it):

```scala
/** 
My Description:

* bullet point
* more points

@param name name desc 
@param othername othername:

* more description points 

@return return val
*/ 
```

Param's are optional and will generate a warning if the names don't match in the validate function or if params are used on a non-lambda expression.

The return value is also optional but would apply to all expressions.

Whilst an incorrect parameter name will be flagged and warned against you won't be forced to put a comment for every parameter.

A couple of helpful utility functions:
```scala
  val (errors, warnings, out, docs, expr) = validate(Left(struct), ruleSuite)

  import com.sparkutils.quality.utils.{RuleSuiteDocs, RelativeWarningsAndErrors}

  val relative = RelativeWarningsAndErrors("../sampleDocsValidation/", errors, warnings)
  val md = RuleSuiteDocs.createMarkdown(docs, ruleSuite, expr, qualityURLGOESHERE+"/sqlfunctions/", Some(relative))

  IOUtils.write(md, new FileOutputStream("./docs/advanced/sampleDocsOutput.md"))

  val emd = RuleSuiteDocs.createErrorAndWarningMarkdown(docs, ruleSuite, relative.copy( relativePath = "../sampleDocsOutput/"))
  IOUtils.write(emd, new FileOutputStream("./docs/advanced/sampleDocsValidation.md"))
```
exist to generate docs of a ruleSuite and validation errors.  The validate function returns both of these inputs.  You must specify the quality url containing the sqlfunction documentation in order to link, hrefs are not carried across mike links yet. 

The [sample docs](sampleDocsOutput.md) and [sample errors/warnings](sampleDocsValidation.md) are generated from the DocMarkdownTest.