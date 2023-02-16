#include <stdio.h>
#include "mpi.h"
#include <iostream>
#include <string>
#include <fstream>
#include <unordered_map>
#include <utility>
#include <list>
#include <algorithm>
#include <vector>
#include <filesystem>
#include <dirent.h>
#include <sstream>
using namespace std;

std::vector<std::string> getFiles()
{
    std::vector<std::string> files;
    struct dirent *entry;
    DIR *dir = opendir("./date");

    if (dir == NULL)
    {
        return files;
    }
    string str1 = ".";
    string str2 = "..";
    while ((entry = readdir(dir)) != NULL)
    {
        cout << entry->d_name << endl;
        if (entry->d_name != str1 && entry->d_name != str2)
        {
            files.push_back(entry->d_name);
        }
    }
    closedir(dir);

    return files;
}
map<std::string, int, less<string>> read_word_by_word(string filename, map<std::string, int, less<string>> numbers)
{
    fstream file;
    string word;
    string path = "./date/";
    cout << path + filename.c_str();
    file.open(path + filename.c_str());
    while (file >> word)
    {
        word.erase(remove(word.begin(), word.end(), '.'), word.end());
        word.erase(remove(word.begin(), word.end(), ','), word.end());
        word.erase(remove(word.begin(), word.end(), ')'), word.end());
        word.erase(remove(word.begin(), word.end(), '\''), word.end());
        word.erase(remove(word.begin(), word.end(), '"'), word.end());
        word.erase(remove(word.begin(), word.end(), ';'), word.end());
        transform(word.begin(), word.end(), word.begin(), ::tolower);
        if (numbers[word] == 0)
            numbers[word] = 1;
        else
        {
            numbers[word] = numbers[word] + 1;
        }
    }
    file.close();
    return numbers;
}
map<std::string, vector<pair<string, int>>> read_word_by_word_final(string filename, map<std::string, vector<pair<string, int>>, less<string>> finalList)
{
    fstream file;
    string word;
    file.open(filename.c_str());
    while (file >> word)
    {
        string firstWord = word;
        string space = "  ";
        file >> word;
        pair<string, int> pair;
        pair.first = word;
        file >> word;
        // cout << "incercare " << word << endl;
        pair.second = stoi(word);

        auto it = finalList.find(firstWord);
        if (it != finalList.end())
        {
            auto list = it->second;
            auto it_pair = std::find_if(list.begin(), list.end(), [&](const std::pair<std::string, int> &p)
                                        { return p.first != pair.first; });
            if (it_pair != list.end())
            {
                (*it_pair).second += 1;
            }
            else
            {
                vector<std::pair<string, int>> newValue;
                std::pair<string, int> par;
                par.first = pair.second;
                par.second = stoi(pair.first);
                newValue.push_back(par);
                finalList[firstWord] = newValue;
            }
        }
        else
        {
            vector<std::pair<string, int>> newValue;
            std::pair<string, int> par;
            par.first = pair.second;
            par.second = stoi(pair.first);
            newValue.push_back(par);
            finalList[firstWord] = newValue;
        }
        finalList[firstWord].push_back(pair);
    }
    file.close();
    return finalList;
}
int main(int argc, char **argv)
{
    std::vector<std::string> fls = getFiles();
    string fisierPrimit;
    int count, rank;
    int mesaj = 0;
    MPI_Status status;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &count);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == 0) // master
    {

        int next_task = 0;
        while (next_task < 25)
        {
            int j = next_task + 1;
            const char *message = fls[next_task].c_str();
            MPI_Send(message, 9, MPI_CHAR, j, 1, MPI_COMM_WORLD);
            next_task++;
        }
        MPI_Recv(&mesaj, 1, MPI_INT, 25, 0, MPI_COMM_WORLD, &status);
        if (mesaj == 1)
        {
            map<std::string, vector<pair<string, int>>, less<string>> finalList;
            finalList = read_word_by_word_final("beforeReduce.txt", finalList);
            string stx = "\x02";
            string etx = "\x03";
            string eoh = "\x01";
            for (auto it = finalList.begin(); it != finalList.end(); it++)
            {
                ofstream outfile;
                outfile.open("final.txt", ios_base::app);
                outfile << "<" << it->first << ",{";
                for (auto pair : it->second)
                {
                    if (pair.first != stx && pair.first != etx && pair.first != eoh)
                        outfile << pair.first << ":" << pair.second << ",";
                }
                outfile << "}" << std::endl;
            }
        }
    }
    else
    {
        int task_index;
        char file_name[9];
        MPI_Recv(&file_name, 9, MPI_CHAR, 0, 1, MPI_COMM_WORLD, &status);
        cout << "Process " << rank << " received file name " << file_name << endl;
        map<std::string, int> numbers;

        numbers = read_word_by_word(file_name, numbers);

        pair<string, map<std::string, int, less<string>>> pair1;
        pair1.first = file_name;
        pair1.second = numbers;

        list<pair<string, pair<string, int>>> firstList;
        for (auto it = numbers.begin(); it != numbers.end(); it++)
        {
            pair<string, pair<string, int>> newElement;
            newElement.first = pair1.first;
            newElement.second.first = it->first;
            newElement.second.second = it->second;
            firstList.push_back(newElement);
        }
        list<pair<string, pair<string, int>>> secondList;
        for (auto it = numbers.begin(); it != numbers.end(); it++)
        {
            pair<string, pair<string, int>> newElement;
            newElement.first = it->first;
            newElement.second.first = pair1.first;
            newElement.second.second = it->second;
            secondList.push_back(newElement);
            ofstream outfile;
            outfile.open("beforeReduce.txt", ios_base::app);
            string space = " ";
            string nothing = "";
            if (it->first != space && it->first != nothing && pair1.first != space)
                outfile << it->first << " " << pair1.first << " " << it->second << " ";
        }
        int mesaj = 1;
        MPI_Send(&mesaj, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    MPI_Finalize();
    return 0;
}