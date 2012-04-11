#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <time.h>

using std::vector;
using std::string;
using std::cout;
using std::cin;
using std::ofstream;
using std::ifstream;

typedef vector<vector<bool> > Cells;

bool GetRandomBool() {
    return rand() > RAND_MAX / 2;
}

void WriteToFile(const Cells& cells, string path) {
    ofstream file(path.c_str(), std::ios::trunc);
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            if (cells[rowIndex][columnIndex]) {
                file << "# ";
            } else {
                file << "~ ";
            }
        }
        file << "\n";
    }
    file.close();
}

void WriteToConsole(const Cells& cells) {
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            if (cells[rowIndex][columnIndex]) {
                cout << "# ";
            } else {
                cout << "~ ";
            }
        }
        cout << "\n";
    }
}

Cells ReadCellsFromFile(string path) {
    Cells cells;
    ifstream file(path.c_str());
    string line;
    while (file) {
        std::getline(file, line);
        vector<bool> cellsRow;
        for (int i = 0; i < line.size(); i += 2) {
            if (line[i] == '~') {
                cellsRow.push_back(false);
            } else if (line[i] == '#') {
                cellsRow.push_back(true);
            }
        }
        if (!cellsRow.empty()) {
            cells.push_back(cellsRow);
        }
    }
    file.close();
    return cells;
}

Cells CreateEmptyCells(int rowCount, int columnCount) {
    Cells cells(rowCount, vector<bool>(columnCount, false));
    return cells;
}

Cells CreateRandomCells(int rowCount, int columnCount) {
    Cells cells;
    for (int i = 0; i < rowCount; i++) {
        vector<bool> row;
        for (int j = 0; j < columnCount; j++) {
            row.push_back(GetRandomBool());
        }
        cells.push_back(row);
    }
    return cells;
}

int modulus(int i, int mod) {
    if (i < 0) {
        return modulus(i + mod, mod);
    }
    return i % mod;
}

int GetNumberOfNeighbours(const Cells& cells, int rowIndex, int columnIndex) {
    int numberOfNeighbours = 0;
    int numberOfRows = cells.size();
    int numberOfColumns = cells[0].size();
    int i;
    int j;
    /*
     ---
     -0-
     ###
     */
    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    /*
     ---
     #0#
     ---
     */
    i = modulus(rowIndex, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    /*
     ###
     -0-
     ---
     */
    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    return numberOfNeighbours;
}

void DoOneLifeTimeStep(Cells* source, Cells* destitation) {
    for (int rowIndex = 0; rowIndex < source->size(); rowIndex++) {
        for (int columnIndex = 0; columnIndex < source->at(0).size(); columnIndex++) {
            int numberOfNeighbours = GetNumberOfNeighbours(*source, rowIndex, columnIndex);
            bool newValue;
            if (source->at(rowIndex).at(columnIndex)) {
                if (numberOfNeighbours < 2 || numberOfNeighbours > 3) {
                    newValue = false;
                } else {
                    newValue = true;
                }
            } else {
                if (numberOfNeighbours == 3) {
                    newValue = true;
                } else {
                    newValue = false;
                }
            }
            destitation->at(rowIndex).at(columnIndex) = newValue;
        }
    }
}

void WriteNumberOfNeighboursToConsole(const Cells& cells) {
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            cout << GetNumberOfNeighbours(cells, rowIndex, columnIndex) << " ";
        }
        cout << "\n";
    }
}

vector<vector<int> > CreateTempArray(const Cells& cells) {
    vector<vector<int> > array(cells.size(), vector<int>(cells[0].size(), 0));
    return array;
}

void DoManyLifeSteps(Cells* source, int n) {
    Cells tempCells(*source);
    Cells* a = source;
    Cells* b = &tempCells;
    for (int i = 0; i < n; i++) {
        Cells* t;
        DoOneLifeTimeStep(a, b);
        t = b;
        b = a;
        a = t;
    }
    if (n % 2 == 1) {
        source-> clear();
        std::copy(tempCells.begin(), tempCells.end(), std::back_inserter(*source));
    }
}

int main(int argc, char **argv) {
    Cells cells = ReadCellsFromFile("cells");
    time_t start = clock();
    DoManyLifeSteps(&cells, 100);
    time_t end = clock();
    cout << (static_cast<double>(end - start) / CLOCKS_PER_SEC) << "\n";
    return 0;
}
