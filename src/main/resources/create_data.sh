date=$1
count=$2

if [ -z "$date" ]
then
  date=`date +'%F'`
fi

if [ -z "$count" ]
then
  count=10
fi

file="data-${date}.txt"
if [ -f $file ]
then
  rm -f $file
fi

# echo "$date, $count"

for((i=1;i<=${count};i++))
do
  sep=","
  id=$i
  name="name${id}"
  remark="some thing to test in hive and presto"
  # echo "${id}${sep}${date}${sep}${name}${sep}${remark}"
  echo "${id}${sep}${date}${sep}${name}${sep}${remark}" >> $file
done