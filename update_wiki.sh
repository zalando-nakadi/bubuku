set -x #echo on
cd ..
git clone "https://github.com/zalando-nakadi/bubuku.wiki.git"
cp -rf bubuku/cli_docs/cli.md bubuku.wiki/cli.md
cd bubuku.wiki
git add .
git commit -m "Generated Wiki via Travis-CI"
git push
cd ../bubuku
set +x #echo off
