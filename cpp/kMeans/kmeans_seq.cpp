#include <iostream>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <math.h>
#include <limits>
#include <string>
#include <fstream>
#include <stdlib.h>
#include <time.h>

using std::vector;
using std::string;
using std::cout;
using std::ofstream;

typedef vector<double> Point;

void PrintNumbers(const vector<int> numbers) {
    cout << "Numbers{";
    for (size_t i = 0; i < numbers.size(); ++i) {
        cout << numbers[i];
    }
    cout << "}";
}

double UniformRandom() {
    return static_cast<double> (rand() + 1) / RAND_MAX;
}

// exponential distribution

double ExponentialRandom(double lambda) {
    double uniRandom = UniformRandom();
    return -1.0 / lambda * log(1 - uniRandom);
}

double SignRandom() {
    double sign = UniformRandom() > 0.5 ? 1.0 : -1.0;
    return sign;
}

Point CreateRandomPoint(const Point& mean, double dispersion) {
    Point point;
    for (size_t i = 0; i < mean.size(); ++i) {
        double rnd = ExponentialRandom(1.0 / sqrt(dispersion)) * SignRandom();
        point.push_back(mean[i] + rnd);
    }
    return point;
}

Point CreateNullPoint(int dimension) {
    Point point;
    for (int i = 0; i < dimension; ++i) {
        point.push_back(0.0);
    }
    return point;
}

vector<Point> CreateRandomPoints(int numberOfPoints, int numberOfClusters,
        int dimension) {
    vector<Point> points;
    Point nullPoint = CreateNullPoint(dimension);
    for (int clusterId = 0; clusterId < numberOfClusters; ++clusterId) {
        Point clusterMean = CreateRandomPoint(nullPoint, 5.0);
        for (int i = 0; i < numberOfPoints / numberOfClusters; ++i) {
            points.push_back(CreateRandomPoint(clusterMean, 1.0));
        }
    }
    return points;
}

void PrintPoint(const Point& point) {
    cout << "Point{";
    for (size_t i = 0; i < point.size(); ++i) {
        cout << point[i] << " ";
    }
    cout << "}\n";
}

void PrintPoints(const vector<Point>& points) {
    cout << "Points:\n";
    for (size_t i = 0; i < points.size(); ++i) {
        PrintPoint(points[i]);
    }
}

double SquareDist(const Point& point1, const Point& point2) {
    double sum = 0.0;
    for (size_t i = 0; i < point1.size(); ++i) {
        double diff = point1[i] - point2[i];
        sum += diff * diff;
    }
    return sum;
}

void AddPoint(Point& a, const Point& b) {
    for (size_t i = 0; i < a.size(); ++i) {
        a[i] += b[i];
    }
}

void MultPoint(Point& a, double b) {
    for (size_t i = 0; i < a.size(); ++i) {
        a[i] *= b;
    }
}

vector<Point> CalculateMeans(const vector<Point>& points,
        const vector<int>& pointsClusterIds,
        int numberOfClusters) {
    int dimension = points[0].size();
    vector<Point> means(numberOfClusters, CreateNullPoint(dimension));
    for (int clusterId = 0; clusterId < numberOfClusters; ++clusterId) {
        Point sum = CreateNullPoint(points[0].size());
        int numberOfPointsInCluster = 0;
        for (size_t i = 0; i < points.size(); ++i) {
            if (pointsClusterIds[i] == clusterId) {
                AddPoint(sum, points[i]);
                ++numberOfPointsInCluster;
            }
        }
        MultPoint(sum, 1.0 / numberOfPointsInCluster);
        means[clusterId] = sum;
    }
    return means;
}

int FindClosestPointIndex(const vector<Point>& points, const Point& point) {
    double minSquareDist = std::numeric_limits<double>::max();
    int closestPointIndex = -1;
    for (int i = 0; i < points.size(); ++i) {
        double squareDist = SquareDist(points[i], point);
        if (minSquareDist > squareDist) {
            minSquareDist = squareDist;
            closestPointIndex = i;
        }
    }
    return closestPointIndex;
}

vector<int> ClusterDataUsingKMeans(const vector<Point>& points,
        int numberOfClusters) {
    vector<Point> meanPoints;
    vector<int> pointsClusterIds(points.size(), -1);
    for (int i = 0; i < numberOfClusters; ++i) {
        meanPoints.push_back(points[i]);
    }
    bool thereWasChange = true;
    while (thereWasChange) {
        thereWasChange = false;
        for (size_t pointIndex = 0; pointIndex < points.size(); ++pointIndex) {
            int pointClusterId = FindClosestPointIndex(meanPoints, points[pointIndex]);
            if (pointsClusterIds[pointIndex] != pointClusterId) {
                pointsClusterIds[pointIndex] = pointClusterId;
                thereWasChange = true;
            }
        }
        meanPoints = CalculateMeans(points, pointsClusterIds, numberOfClusters);
    }
    return pointsClusterIds;
}

void WriteToFile(const vector<int>& numbers, string path) {
    ofstream file(path.c_str());
    for (int i = 0; i < numbers.size(); i++) {
        file << numbers[i] << "\n";
    }
    file.close();
}

int main(int argc, char *argv[]) {
    srand(13);
    vector<Point> randomPoints = CreateRandomPoints(30000, 30, 20);
    time_t start = clock();
    vector<int> pointsClusterIds = ClusterDataUsingKMeans(randomPoints, 30);
    time_t end = clock();
    cout << (static_cast<double>(end - start) / CLOCKS_PER_SEC) << "\n";
    WriteToFile(pointsClusterIds, "output");
}

