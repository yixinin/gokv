rm node*/cmd
rm -rf node*/Data/*
rm -rf node*/Logs/*
go build
cp cmd node3/cmd
cp cmd node2/cmd
cp cmd node1/cmd