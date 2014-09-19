It seems like the following workflow is actually able to realize the desired versioning in git:

* Version on the develop branch is
  `<last-release>+dev`.
* When releasing a new version from the develop branch:
    * Change version numbers in files to
      `<next-release>`
      and commit.
    * Create the branch
      `releases/v<next-release>`
      from develop.
    * On develop, change version numbers in files to
      `<next-release>+dev`
      and commit.
    * When release is complete, merge the
      `releases/v<next-release>`
      branch into master and tag it.

This way,

* it is easy to know which release version the current dev code is
  based on,
* a jar file created on the develop-based branch is easily detectable
  as development version,
* while	the commits on the release branch can still be merged back to
  the develop branch.
