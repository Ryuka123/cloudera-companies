-------------------------------------------------------------------------------
Cloudera Companies Build Instructions
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
Build
-------------------------------------------------------------------------------

mvn clean install -PDEV,ITR

-------------------------------------------------------------------------------
Package
-------------------------------------------------------------------------------

mvn clean install -PREL,PKG

-------------------------------------------------------------------------------
Release
-------------------------------------------------------------------------------

git checkout -b maintenance-1.0.0
mvn --batch-mode release:prepare
mvn release:perform -Dgoals=install
git checkout master
git merge maintenance-1.0.0
mvn versions:set -DnewVersion=1.1.0-SNAPSHOT -DgenerateBackupPoms=false
git commit -a -m "Preparing for next major release: updated version in master"