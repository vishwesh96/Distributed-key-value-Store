gnome-terminal -x go run chord-create.go &
for i in `seq 1 $1`
do
gnome-terminal -x go run chord-join.go $i &
done;