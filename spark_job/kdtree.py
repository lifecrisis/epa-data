class KDTree:

    def __init__(self, points):
        self.dimension = len(points[0].location())
        self.root = None
        self.add_all(points)

    def add(self, point):
        new_node = KDNode(point)
        if not self.root:
            self.root = new_node
        else:
            current = self.root
            depth = 0
            while True:
                axis = depth % self.dimension
                if new_node.location[axis] <= current.location[axis]:
                    if current.left:
                        current = current.left
                    else:
                        current.left = new_node
                        break
                else:
                    if current.right:
                        current = current.right
                    else:
                        current.right = new_node
                        break
                depth += 1

    def add_all(self, points, depth=0):
        if len(points) == 0:
            return
        axis = depth % self.dimension
        points.sort(key=lambda p: p.location()[axis])
        median = len(points) // 2
        self.add(points[median])
        self.add_all(points[:median], depth + 1)
        self.add_all(points[median + 1:], depth + 1)

    def query(self, point, k=1):
        """ Return a list of the k KDNode objects nearest to point. """

        def search_node(bpq, curr, depth, point):
            """ Recursive helper search method. """
            if curr is None:
                return
            bpq.add(curr, point.distance(curr.location))
            axis = depth % self.dimension
            if point.location()[axis] < curr.location[axis]:
                search_node(bpq, curr.left, depth + 1, point)
                searched = 'LEFT'
            else:
                search_node(bpq, curr.right, depth + 1, point)
                searched = 'RIGHT'
            diff = abs(curr.location[axis] - point.location()[axis])
            c1 = len(bpq) < k
            c2 = diff < bpq.contents[-1][1]
            if (c1 or c2):
                if searched == 'LEFT':
                    search_node(bpq, curr.right, depth + 1, point)
                else:
                    search_node(bpq, curr.left, depth + 1, point)

        result = BoundedPriorityQueue(k)
        search_node(result, self.root, 0, point)

        result = [x[0] for x in result.contents]
        return result


class KDNode:

    def __init__(self, point):
        self.location = point.location()
        self.value = point.value()
        self.left = None
        self.right = None

    def __repr__(self):
        pass

    def __str__(self):
        pass


class BoundedPriorityQueue:

    def __init__(self, member_max):
        self.contents = list()
        self.member_max = member_max

    def add(self, kdnode, priority):
        if len(self.contents) < self.member_max:
            self.contents.append((kdnode, priority))
            self.contents = sorted(self.contents, key=lambda e: e[1])
        else:
            index = 0
            for element in self.contents:
                if priority < element[1]:
                    self.contents.insert(index, (kdnode, priority))
                    self.contents.pop()
                    break
                index += 1

    def __len__(self):
        """ Return the length of this BoundedPriorityQueue. """
        return len(self.contents)
